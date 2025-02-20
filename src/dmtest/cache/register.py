import dmtest.cache.small_config_tests as small_config_tests
import dmtest.cache.resize_origin_tests as resize_origin_tests

def register(tests):
    small_config_tests.register(tests)
    resize_origin_tests.register(tests)
