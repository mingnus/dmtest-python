import toml


_CONFIG_KEYS = {
    "metadata_dev":        ("devices", None),
    "data_dev":            ("devices", None),
    "cache_dev":           ("devices", None),
    "disable_by_id_check": ("devices", False),
    "cache_policy":        ("run", "smq"),
    "tags":                ("run", None),
}


class Config:
    def __init__(self, raw):
        self._raw = raw

    def get(self, key):
        entry = _CONFIG_KEYS.get(key)
        if entry is None:
            raise KeyError(f"unknown config key: {key}")
        section, default = entry
        return self._raw.get(section, {}).get(key, default)


# Linux reordered my nvme drives once and I ran tests across
# /boot.  This check tries to avoid that.  The exception is
# virt devices, which don't seem to have an id.
def _check_dev(value, name):
    if not (
        value.startswith("/dev/vd") or value.startswith("/dev/mapper/")
    ) and not value.startswith("/dev/disk/by-id/"):
        raise ValueError(f"config value '{name}' does not begin with /dev/disk/by-id")


def _validate(cfg):
    if "devices" not in cfg._raw:
        raise ValueError(
            "config.toml must have a [devices] section; "
            "see config.toml.example for the expected format"
        )
    for key in ("metadata_dev", "data_dev"):
        if cfg.get(key) is None:
            raise ValueError(f"config.toml: missing required key '{key}' in [devices]")
    if not cfg.get("disable_by_id_check"):
        _check_dev(cfg.get("metadata_dev"), "metadata_dev")
        _check_dev(cfg.get("data_dev"), "data_dev")


def read_config(path="config.toml"):
    with open(path, "r") as f:
        raw = toml.load(f)
    cfg = Config(raw)
    _validate(cfg)
    return cfg
