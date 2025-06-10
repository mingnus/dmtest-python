import dmtest.process as process
import dmtest.units as units

# convenient method for extracting necessary parameters from the stack
def cache_params(stack):
    block_size = stack.block_size()
    params = {
        "block_size": block_size,
        "cache_blocks": stack.cache_size() // block_size,
        "origin_blocks": stack.target_len() // block_size,
        "metadata_version": stack.metadata_version(),
    }
    return params


def prepare_populated_cache(cmeta, **opts):
    block_size = opts.get("block_size", units.kilo(32))
    cache_blocks = opts.get("cache_blocks", 1024)
    origin_blocks = opts.get("origin_blocks", cache_blocks)

    residency = opts.get("residency", 80)
    dirty_percentage = opts.get("dirty_percentage", 0)
    clean_shutdown = opts.get("clean_shutdown", True)

    # default to v2 metadata, matching LVM's preset
    metadata_version = opts.get("metadata_version", 2)

    process.run(f"pdata_tools_dev cache_generate_metadata --format -o {cmeta}"
                f" --cache-block-size {block_size}"
                f" --nr-cache-blocks {cache_blocks}"
                f" --nr-origin-blocks {origin_blocks}"
                f" --metadata-version {metadata_version}"
                f" --percent-dirty {dirty_percentage}"
                f" --percent-resident {residency}")

    if not clean_shutdown:
        process.run(f"pdata_tools_dev cache_generate_metadata -o {cmeta}"
                     " --set-clean-shutdown false")

