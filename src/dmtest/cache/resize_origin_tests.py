import dmtest.units as units
import dmtest.utils as utils
import logging as log
import math
import random
import struct
import subprocess
import unittest
import xml.etree.ElementTree as ET

from dmtest.assertions import assert_equal
from dmtest.cache_stack import ManagedCacheStack, CachePolicy
from dmtest.process import run

#----------------------------------------------------------------

def generate_tail_mapped_xml(f, block_size, nr_cache_blocks, nr_origin_blocks, policy_name, dirty = False):
    mapped_begin = nr_origin_blocks - min(nr_cache_blocks, nr_origin_blocks)
    oblocks = list(range(mapped_begin, nr_origin_blocks))
    random.shuffle(oblocks)
    f.write(f"<superblock uuid=\"\" block_size=\"{block_size}\""
            f" nr_cache_blocks=\"{nr_cache_blocks}\" policy=\"{policy_name}\" hint_width=\"4\">\n")
    f.write(f"  <mappings>\n")
    for (cblock, oblock) in enumerate(oblocks):
        flag = str(dirty).lower()
        f.write(f"    <mapping cache_block=\"{cblock}\" origin_block=\"{oblock}\" dirty=\"{flag}\"/>\n")
    f.write(f"  </mappings>\n")
    f.write("</superblock>\n")
    f.flush()

def get_discard_bitset_size(cmeta):
    with open(cmeta, "rb") as f:
        f.seek(216)
        buf = f.read(16)
        return struct.unpack("<2Q", buf)

def check_mappings_truncation(cmeta, old_cache_dump, new_nr_origin_blocks):
    tree_old = ET.parse(old_cache_dump)
    root_old = tree_old.getroot()

    cdump = utils.TempFile();
    run(f"cache_dump -o {cdump.path} {cmeta}")
    tree = ET.parse(f"{cdump.path}")
    root = tree.getroot()
    mappings_trunc = root.find("mappings").iter("mapping")

    for mapping in root_old.find("mappings").iter("mapping"):
        if int(mapping.attrib["origin_block"]) < new_nr_origin_blocks:
            mapping_new = next(mappings_trunc)
            assert_equal(mapping_new.attrib["cache_block"], mapping.attrib["cache_block"])
            assert_equal(mapping_new.attrib["origin_block"], mapping.attrib["origin_block"])
            assert_equal(mapping_new.attrib["dirty"], mapping.attrib["dirty"])

    try:
        mapping_new = next(mappings_trunc)
    except StopIteration:
        pass
    else:
        raise Exception("unexpected mappings in truncated cache")


def check_sized_metadata(cmeta, old_cache_dump, new_origin_size):
    # ensure the discard bitset size was changed according to the cache target length
    # (linux.git commit #235d2e7)
    (discard_block_size, discard_nr_blocks) = get_discard_bitset_size(cmeta);
    assert_equal(discard_nr_blocks, math.ceil(new_origin_size / discard_block_size))
    # check truncated mappings
    new_nr_origin_blocks = new_origin_size // block_size
    check_mappings_truncation(cmeta, old_cache_dump, new_nr_origin_blocks)


def expand_origin_with_reload(fix, policy_name):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    block_size = units.kilo(32)
    cache_size = units.meg(128)
    origin_size = units.gig(1)
    nr_cache_blocks = cache_size // block_size
    nr_origin_blocks = origin_size // block_size

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        format = False,
        metadata_size = units.meg(4),
        block_size = block_size,
        cache_size = cache_size,
        target_len = origin_size,
        policy = CachePolicy(policy_name),
    )

    cdump = utils.TempFile();

    with stack.activate_support_devs() as (cmeta, cdata):
        generate_tail_mapped_xml(cdump.file, block_size, nr_cache_blocks, nr_origin_blocks,
                                 policy_name)
        run(f"cache_restore -i {cdump.path} -o {cmeta}")

    new_origin_size = units.gig(4)

    # expand origin with table reload
    with stack.activate():
        stack.resize_origin(new_origin_size)

    with stack.activate_support_devs() as (cmeta, cdata):
        check_sized_metadata(cmeta, cdump.path, new_origin_size)


def shrink_origin_with_reload_drops_mappings(fix, policy_name):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    block_size = units.kilo(32)
    cache_size = units.meg(128)
    origin_size = units.gig(4)

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        format = False,
        metadata_size = units.meg(4),
        block_size = block_size,
        cache_size = cache_size,
        target_len = origin_size,
        policy = CachePolicy(policy_name),
    )

    cdump = utils.TempFile();

    with stack.activate_support_devs() as (cmeta, cdata):
        nr_cache_blocks = cache_size // block_size
        nr_origin_blocks = origin_size // block_size
        generate_tail_mapped_xml(cdump.file, block_size, nr_cache_blocks, nr_origin_blocks,
                                 policy_name)
        run(f"cache_restore -i {cdump.path} -o {cmeta}")

    reduced_size = cache_size // 2
    new_origin_size = origin_size - reduced_size

    # shrink origin with table reload
    with stack.activate():
        stack.resize_origin(new_origin_size)

    with stack.activate_support_devs() as (cmeta, cdata):
        check_sized_metadata(cmeta, cdump.path, new_origin_size)


# Actually there's no differences between teardown and reload while shrinking
# the origin, as we always have to load a new dm-cache table to change the
# target length. Here we test both the approaches to ensure test coverage.
def shrink_origin_with_teardown_drops_mappings(fix, policy_name):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    block_size = units.kilo(32)
    cache_size = units.meg(128)
    origin_size = units.gig(4)

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        format = False,
        metadata_size = units.meg(4),
        block_size = block_size,
        cache_size = cache_size,
        target_len = origin_size,
        policy = CachePolicy(policy_name),
    )

    cdump = utils.TempFile();

    with stack.activate_support_devs() as (cmeta, cdata):
        nr_cache_blocks = cache_size // block_size
        nr_origin_blocks = origin_size // block_size
        generate_tail_mapped_xml(cdump.file, block_size, nr_cache_blocks, nr_origin_blocks,
                                 policy_name)
        run(f"cache_restore -i {cdump.path} -o {cmeta}")

    reduced_size = cache_size // 2
    new_origin_size = origin_size - reduced_size

    # activate the cache whilst shrinking the origin
    stack.resize_origin(new_origin_size)
    with stack.activate():
        pass

    with stack.activate_support_devs() as (cmeta, cdata):
        check_sized_metadata(cmeta, cdump.path, new_origin_size)


def shrink_origin_with_reload_should_fail_if_blocks_dirty(fix, policy_name):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    block_size = units.kilo(32)
    cache_size = units.meg(128)
    origin_size = units.gig(4)

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        format = False,
        metadata_size = units.meg(4),
        block_size = block_size,
        cache_size = cache_size,
        target_len = origin_size,
        policy = CachePolicy(policy_name, migration_threshold = 0),
    )

    cdump = utils.TempFile();

    with stack.activate_support_devs() as (cmeta, cdata):
        nr_cache_blocks = cache_size // block_size
        nr_origin_blocks = origin_size // block_size
        generate_tail_mapped_xml(cdump.file, block_size, nr_cache_blocks, nr_origin_blocks,
                                 policy_name, dirty = True)
        run(f"cache_restore -i {cdump.path} -o {cmeta}")

    reduced_size = cache_size // 2
    new_origin_size = origin_size - reduced_size

    # try shrinking the origin with table reload
    with stack.activate():
        try:
            stack.resize_origin(new_origin_size) # should fail due to dirty blocks
        except subprocess.CalledProcessError as e:
            if e.returncode != errno.EFBIG:
                raise
        else:
            raise Exception("shrink cache origin succeeded without error")


def shrink_origin_with_teardown_should_fail_if_blocks_dirty(fix, policy_name):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    block_size = units.kilo(32)
    cache_size = units.meg(128)
    origin_size = units.gig(4)

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        format = False,
        metadata_size = units.meg(4),
        block_size = block_size,
        cache_size = cache_size,
        target_len = origin_size,
        policy = CachePolicy(policy_name, migration_threshold = 0),
    )

    cdump = utils.TempFile();

    with stack.activate_support_devs() as (cmeta, cdata):
        nr_cache_blocks = cache_size // block_size
        nr_origin_blocks = origin_size // block_size
        generate_tail_mapped_xml(cdump.file, block_size, nr_cache_blocks, nr_origin_blocks,
                                 policy_name, dirty = True)
        run(f"cache_restore -i {cdump.path} -o {cmeta}")

    reduced_size = cache_size // 2
    new_origin_size = origin_size - reduced_size

    # try activate the cache whilst shrinking the origin
    stack.resize_origin(new_origin_size)
    try:
        with stack.activate(): # should fail due to dirty blocks
            pass
    except subprocess.CalledProcessError as e:
        if e.returncode != errno.EFBIG:
            raise
    else:
        raise Exception("shrink cache origin succeeded without error")


def t_expand_origin_with_reload_mq(fix):
    expand_origin_with_reload(fix, "mq")

def t_expand_origin_with_reload_smq(fix):
    expand_origin_with_reload(fix, "smq")

def t_shrink_origin_with_reload_drops_mappings_mq(fix):
    shrink_origin_with_reload_drops_mappings(fix, "mq")

def t_shrink_origin_with_reload_drops_mappings_smq(fix):
    shrink_origin_with_reload_drops_mappings(fix, "smq")

def t_shrink_origin_with_teardown_drops_mappings_mq(fix):
    shrink_origin_with_teardown_drops_mappings(fix, "mq")

def t_shrink_origin_with_teardown_drops_mappings_smq(fix):
    shrink_origin_with_teardown_drops_mappings(fix, "smq")

def t_shrink_origin_with_reload_should_fail_if_blocks_dirty_mq(fix):
    shrink_origin_with_reload_should_fail_if_blocks_dirty(fix, "mq")

def t_shrink_origin_with_reload_should_fail_if_blocks_dirty_smq(fix):
    shrink_origin_with_reload_should_fail_if_blocks_dirty(fix, "smq")

def t_shrink_origin_with_teardown_should_fail_if_blocks_dirty_mq(fix):
    shrink_origin_with_teardown_should_fail_if_blocks_dirty(fix, "mq")

def t_shrink_origin_with_teardown_should_fail_if_blocks_dirty_smq(fix):
    shrink_origin_with_teardown_should_fail_if_blocks_dirty(fix, "smq")

#----------------------------------------------------------------

def register(tests):
    tests.register_batch(
        "/cache/resize/",
        [
            ("expand_origin_with_reload_mq",
             t_expand_origin_with_reload_mq),
            ("expand_origin_with_reload_smq",
             t_expand_origin_with_reload_smq),
            ("shrink_origin_with_reload_drops_mappings_mq",
             t_shrink_origin_with_reload_drops_mappings_mq),
            ("shrink_origin_with_reload_drops_mappings_smq",
             t_shrink_origin_with_reload_drops_mappings_smq),
            ("shrink_origin_with_teardown_drops_mappings_mq",
             t_shrink_origin_with_teardown_drops_mappings_mq),
            ("shrink_origin_with_teardown_drops_mappings_smq",
             t_shrink_origin_with_teardown_drops_mappings_smq),
            ("shrink_origin_with_reload_should_fail_if_blocks_dirty_mq",
             t_shrink_origin_with_reload_should_fail_if_blocks_dirty_mq),
            ("shrink_origin_with_reload_should_fail_if_blocks_dirty_smq",
             t_shrink_origin_with_reload_should_fail_if_blocks_dirty_smq),
            ("shrink_origin_with_teardown_should_fail_if_blocks_dirty_mq",
             t_shrink_origin_with_teardown_should_fail_if_blocks_dirty_mq),
            ("shrink_origin_with_teardown_should_fail_if_blocks_dirty_smq",
             t_shrink_origin_with_teardown_should_fail_if_blocks_dirty_smq),
        ],
    )
