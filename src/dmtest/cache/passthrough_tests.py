import dmtest.cache.status as status
import dmtest.cache.utils as cache_utils
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

def test_passthrough_never_promotes(fix):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        block_size = units.kilo(64),
        cache_size = units.meg(64),
        target_len = units.gig(1),
        policy = CachePolicy("smq"),
        io_mode = "passthrough",
    )

    with stack.activate() as cache:
        for _ in range(100):
            utils.wipe_device(cache, 640)

        s = status.cache_status(cache)
        assert_equal(0, s["promotions"])
        assert_equal(0, s["cache-used"])


def test_passthrough_demotes_writes(fix):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        block_size = units.kilo(64),
        cache_size = units.meg(64),
        target_len = units.gig(1),
        policy = CachePolicy("smq"),
        io_mode = "passthrough",
        format = False,
    )

    with stack.activate_support_devs() as (cmeta, cdata):
        cache_utils.prepare_populated_cache(cmeta, **cache_utils.cache_params(stack))
        with stack.activate_top_level() as cache:
            utils.wipe_device(cache)

            s = status.cache_status(cache)
            assert_equal(0, s["cache-used"])


def test_passthrough_does_not_demote_reads(fix):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        block_size = units.kilo(64),
        cache_size = units.meg(64),
        target_len = units.gig(1),
        policy = CachePolicy("smq"),
        io_mode = "passthrough",
        format = False,
    )

    with stack.activate_support_devs() as (cmeta, cdata):
        params = cache_utils.cache_params(stack)
        cache_utils.prepare_populated_cache(cmeta, **params, residency=100)
        with stack.activate_top_level() as cache:
            utils.read_device_to_null(cache)

            s = status.cache_status(cache)
            assert_equal(params["cache_blocks"], s["cache-used"])


def test_passthrough_fails_with_dirty_blocks(fix):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        block_size = units.kilo(64),
        cache_size = units.meg(64),
        target_len = units.gig(1),
        policy = CachePolicy("smq"),
        io_mode = "passthrough",
        format = False,
    )

    with stack.activate_support_devs() as (cmeta, cdata):
        cache_utils.prepare_populated_cache(cmeta, **cache_utils.cache_params(stack), dirty_percentage=100)
        try:
            with stack.activate_top_level():
                pass
        except subprocess.CalledProcessError as e:
            pass
        else:
            raise Exception("passthrough mode with dirty blocks succeeded without error")


def t_passthrough_never_promotes(fix):
    test_passthrough_never_promotes(fix)

def t_passthrough_demotes_writes(fix):
    test_passthrough_demotes_writes(fix)

def t_passthrough_does_not_demote_reads(fix):
    test_passthrough_does_not_demote_reads(fix)

def t_passthrough_fails_with_dirty_blocks(fix):
    test_passthrough_fails_with_dirty_blocks(fix)

#----------------------------------------------------------------

def register(tests):
    tests.register_batch(
        "/cache/passthrough/",
        [
            ("passthrough_never_promotes",
             t_passthrough_never_promotes),
            ("passthrough_demotes_writes",
             t_passthrough_demotes_writes),
            ("passthrough_does_not_demote_reads",
             t_passthrough_does_not_demote_reads),
            ("passthrough_fails_with_dirty_blocks",
             t_passthrough_fails_with_dirty_blocks),
        ],
    )
