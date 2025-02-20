from contextlib import contextmanager
from collections import namedtuple
from typing import Optional, Dict, Callable, List

import dmtest.units as units
import dmtest.device_mapper.dev as dmdev
import dmtest.device_mapper.table as table
import dmtest.device_mapper.targets as targets
import dmtest.tvm as tvm
import dmtest.utils as utils
import logging as log

class CachePolicy:
    def __init__(self, name, **args):
        self._name = name
        self._args = args

class CacheStack:
    def __init__(self, fast_dev, origin_dev, metadata_dev, **opts):
        self._fast_dev = fast_dev
        self._origin_dev = origin_dev
        self._metadata_dev = metadata_dev
        format_cache = opts.pop("format", False)

        # cache target length, which could be smaller than the origin device size
        self._target_len = opts.pop("target_len", utils.dev_size(origin_dev))

        self._block_size = opts.pop("block_size", units.kilo(32))
        self._io_mode = opts.pop("io_mode", "writethrough")
        self._metadata_version = opts.pop("metadata_version", 2)
        self._policy = opts.pop("policy", CachePolicy("default"))

        # lazy initialized fields
        self._cache = None

        if format_cache:
            utils.wipe_device(self._metadata_dev, 8)

    def _cache_table(self):
        features = [self._io_mode]
        if self._metadata_version == 2:
            features.append("metadata2")

        return table.Table(
            targets.CacheTarget(
                self._target_len,
                self._metadata_dev,
                self._fast_dev,
                self._origin_dev,
                self._block_size,
                features,
                self._policy._name,
                self._policy._args
            )
        )

    @contextmanager
    def activate(self):
        try:
            with dmdev.dev(self._cache_table()) as cache:
                self._cache = cache
                yield self._cache
        finally:
            self._cache = None

    def change_io_mode(self, new_mode):
        self._io_mode = new_mode

    def change_policy(self, new_policy, **args):
        self._policy = new_policy
        self._policy_args = args

    def reload(self):
        if self._cache is None:
            raise Exception("inactive")

        self._cache.load(self._cache_table())

    def resize(self, new_size: int):
        self._target_len = new_size

        if self._cache is None:
            return

        with self._cache.pause():
            self.reload()

# load the origin into the cache dev, and load empty error targets into the
# origin and ssd
@contextmanager
def uncache(cache):
    with cache._cache.pause() as cdev:
        cdev.load(table.Table(targets.LinearTarget(cache._target_len, cache._origin_dev, 0)))
        yield cdev

# TODO
def wait_for_clean_cache(cache):
    raise NotImplementedError()

# TODO
def prepare_populated_cache(cache):
    raise NotImplementedError()

class ManagedCacheStack:
    def __init__(self, fast_dev, origin_dev, **opts):
        metadata_size = opts.pop("metadata_size", units.meg(4))

        vm = tvm.VM()
        vm.add_allocation_volume(fast_dev);
        vm.add_volume(tvm.LinearVolume("cmeta", metadata_size))

        # allocate the fast device
        cache_dev = opts.pop("cache_dev", None)
        cache_size = opts.pop("cache_size", None)
        if cache_dev is None:
            cache_size = vm.free_space() if cache_size is None else cache_size
            vm.add_volume(tvm.LinearVolume("cdata", cache_size))
        else:
            cache_size = utils.dev_size(cache_dev) if cache_size is None else cache_size
            vm.add_allocation_volume(cache_dev)
            vm.add_volume(tvm.LinearVolume("cdata", cache_size), lambda seg: seg.dev == cache_dev)

        self._vm = vm
        self._cache_dev = cache_dev
        self._origin_dev = origin_dev
        self._opts = opts

        # lazy initialized fields
        self._support_devs = None
        self._top_level = None

    @contextmanager
    def activate_support_devs(self):
        if self._support_devs is not None:
            raise Exception("already activated")

        try:
            cmeta_table = self._vm.table("cmeta")
            cdata_table = self._vm.table("cdata")
            with dmdev.dev(cmeta_table) as cmeta, dmdev.dev(cdata_table) as cdata:
                self._support_devs = (cmeta, cdata)
                yield self._support_devs
        finally:
            self._support_devs = None

    @contextmanager
    def activate_top_level(self):
        if self._support_devs is None:
            raise Exception("inactive support devices")

        if self._top_level is not None:
            raise Exception("already activated")

        try:
            self._top_level = CacheStack(self._support_devs[1], self._origin_dev, self._support_devs[0], **self._opts)
            with self._top_level.activate() as cache:
                yield cache
        finally:
            self._top_level = None

    @contextmanager
    def activate(self):
        with self.activate_support_devs():
            with self.activate_top_level() as cache:
                yield cache

    def resize_cache_dev(self, new_size):
        if (self._top_level is None) ^ (self._support_devs is None):
            raise Exception("inactive top level or supported devs")

        is_expand = new_size > self._vm.size("cdata")

        self._vm.resize(
            "cdata",
            new_size,
            {lambda seg: seg.dev == self.cache_dev} if self.cache_dev is not None else None
        )

        if self._top_level is None:
            return

        with self._top_level._cache.pause():
            with self._support_devs[1].pause():
                self._support_devs[1].load(self._vm.table("cdata"))

            if is_expand:
                self._top_level.reload()

    def resize_origin(self, new_size):
        self._opts["target_len"] = new_size

        if self._top_level is None:
            return

        self._top_level.resize(new_size)
