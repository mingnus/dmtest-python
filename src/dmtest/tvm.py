from collections import namedtuple
from typing import List, Tuple, Optional, Dict, Callable

import dmtest.device_mapper.table as table
import dmtest.device_mapper.targets as targets
import dmtest.utils as utils


Segment = namedtuple("Segment", ["dev", "offset", "length"])


class SegmentAllocationError(Exception):
    """Raised when there is not enough space for allocating segments."""


class VolumeError(Exception):
    """Raised when there is an issue related to volume management."""


def _allocate_segment(size: int, segs: List[Segment]) -> Tuple[Segment, List[Segment]]:
    """
    Allocates a single segment with a length not greater than 'size'.

    Args:
        size (int): The maximum size of the segment to allocate.
        segs (List[Segment]): A list of available Segments.

    Returns:
        Tuple[Segment, List[Segment]]: A tuple containing the newly allocated segment and the remaining segments.
    """
    if len(segs) == 0:
        raise SegmentAllocationError("Out of space in the segment allocator")
    s = segs.pop(0)
    if s.length > size:
        segs.insert(0, Segment(s.dev, s.offset + size, s.length - size))
        s = Segment(s.dev, s.offset, size)
    return (s, segs)


def _merge(segs: List[Segment]) -> List[Segment]:
    segs.sort(key=lambda seg: (seg.dev, seg.offset))

    merged = []
    s = segs.pop(0)
    while segs:
        n = segs.pop(0)
        if (n.dev == s.dev) and (n.offset == (s.offset + s.length)):
            # adjacent, we can merge them
            s = Segment(s.dev, s.offset, s.length + n.length)
        else:
            # non-adjacent, push what we've got
            merged.append(s)
            s = n
    if s:
        merged.append(s)

    return merged


class Allocator:
    def __init__(self):
        self._free_segments: List[Segment] = []

    def allocate_segments(
        self,
        size: int,
        segment_predicate: Optional[Callable[[Segment], bool]] = None,
    ) -> List[Segment]:
        if segment_predicate:
            segments = [s for s in self._free_segments if segment_predicate(s)]
        else:
            segments = self._free_segments

        result = []
        while size > 0:
            (s, segments) = _allocate_segment(size, segments)
            size -= s.length
            result.append(s)

        self._free_segments = segments
        return result

    def release_segments(self, segs: List[Segment]):
        self._free_segments += segs
        self._free_segments = _merge(self._free_segments)

    def free_space(self) -> int:
        return sum([seg.length for seg in self._free_segments])


class Volume:
    def __init__(self, name: str, length: int):
        self._name = name
        self._length = length
        self._segments: List[Segment] = []
        self._targets: List[targets.LinearTarget] = []
        self._allocated = False

    def size(self) -> int:
        return sum(seg.length for seg in self._segments)

    def resize(
        self,
        allocator,
        new_length,
        segment_predicate: Optional[Callable[[Segment], bool]] = None,
    ):
        raise NotImplementedError()

    def allocate(
        self,
        allocator,
        segment_predicate: Optional[Callable[[Segment], bool]] = None,
    ):
        raise NotImplementedError()


def _segs_to_targets(segs):
    return [targets.LinearTarget(s.length, s.dev, s.offset) for s in segs]


class LinearVolume(Volume):
    def __init__(self, name: str, length: int):
        super().__init__(name, length)

    def resize(
        self,
        allocator,
        new_length,
        segment_predicate: Optional[Callable[[Segment], bool]] = None,
    ):
        if not self._allocated:
            self._length = new_length
            return

        if new_length < self._length:
            raise NotImplementedError("reduce not implemented")

        new_segs = allocator.allocate_segments(new_length - self._length, segment_predicate)
        self._segments += new_segs
        self._targets += _segs_to_targets(new_segs)
        self._length = new_length

    def allocate(
        self,
        allocator,
        segment_predicate: Optional[Callable[[Segment], bool]] = None,
    ):
        self._segments = allocator.allocate_segments(self._length, segment_predicate)
        self._targets = _segs_to_targets(self._segments)
        self._allocated = True


# This class manages the allocation aspect of volume management.
# It generates dm tables, but does _not_ manage activation.  Use
# the usual `with dmdev.dev(table) as thin:` method for that
class VM:
    def __init__(self):
        self._allocator = Allocator()
        self._volumes: Dict[str, Volume] = {}

    def add_allocation_volume(
        self, dev: str, offset: int = 0, length: Optional[int] = None
    ) -> None:
        if not length:
            length = utils.dev_size(dev)

        self._allocator.release_segments([Segment(dev, offset, length)])

    def free_space(self) -> int:
        return self._allocator.free_space()

    def add_volume(
        self,
        vol: Volume,
        segment_predicate: Optional[Callable[[Segment], bool]] = None,
    ) -> None:
        self._check_not_exist(vol._name)
        vol.allocate(self._allocator, segment_predicate)
        self._volumes[vol._name] = vol

    def remove_volume(self, name: str) -> None:
        self._check_exists(name)
        vol = self._volumes[name]
        self._allocator.release_segments(vol._segments)
        del self._volumes[name]

    def size(self, name: str) -> int:
        return self._volumes[name].size()

    def resize(
        self,
        name: str,
        new_size: int,
        segment_predicate: Optional[Callable[[Segment], bool]] = None,
    ) -> None:
        self._check_exists(name)
        self._volumes[name].resize(self._allocator, new_size, segment_preicate)

    def segments(self, name: str) -> List[Segment]:
        self._check_exists(name)
        return self._volumes[name]._segments

    def targets(self, name: str) -> List[targets.LinearTarget]:
        self._check_exists(name)
        return self._volumes[name]._targets

    def table(self, name: str) -> table.Table:
        return table.Table(*self.targets(name))

    def _check_not_exist(self, name: str) -> None:
        if name in self._volumes:
            raise VolumeError(f"Volume '{name}' already exists")

    def _check_exists(self, name: str) -> None:
        if name not in self._volumes:
            raise VolumeError(f"Volume '{name}' doesn't exist")
