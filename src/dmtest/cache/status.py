import re

def _parse_usage(str):
    (used, total) = str.split("/")
    return (int(used), int(total))


def _parse_feature_flags(h, toks):
    nr_flags = int(toks[0])

    h["io-mode"] = "writeback"
    h["metadata-version"] = 1
    h["discard-passdown"] = True

    for t in toks[1:nr_flags + 1]:
        if t == "writethrough" or t == "passthrough":
            h["io-mode"] = t

        elif t == "no_discard_passdown":
            h["discard-passdown"] = False

        elif t == "metadata2":
            h["metadata-version"] = 2

        else:
            raise ValueError(f"Bad cache feature {t}")

    return nr_flags + 1


def _parse_core_args(h, toks):
    nr_args = int(toks[0])
    if nr_args % 2 != 0:
        raise ValueError(f"Bad number of core args {nr_args}")

    it = iter(toks[1:nr_args + 1])
    for key, value in zip(it, it):
        if key == "migration_threshold":
            h[key] = int(value)
        else:
            raise ValueError(f"Bad cache core args {key}")

    return nr_args + 1


def _parse_policy_name(h, tok):
    if tok == "mq" or tok == "smq":
        return tok

    raise ValueError(f"Bad cache policy name {tok}")


def _parse_policy_config_values(h, toks):
    nr_args = int(toks[0])
    if nr_args % 2 != 0:
        raise ValueError(f"Bad number of policy args {nr_args}")

    it = iter(toks[1:nr_args + 1])
    for key, value in zip(it, it):
        h[key] = value

    return nr_args + 1


def _parse_mode(h, tok):
    if tok == "ro":
        h["mode"] = "read-only"

    elif tok == "rw":
        h["mode"] = "read-write"

    else:
        raise ValueError(f"Bad cache mode {tok}")


def _parse_needs_check(str):
    return str == "needs_check"


def _parse_cache_status(str):
    tokens = re.split(r"\s+", str)[3:]
    h = {}

    if tokens[0] == "Fail":
        h["mode"] = "fail"
        return h

    h["metadata-block-size"] = int(tokens[0])

    (used, total) = _parse_usage(tokens[1])
    h["metadata-used"] = used
    h["metadata-total"] = total

    h["cache-block-size"] = int(tokens[2])

    (used, total) = _parse_usage(tokens[3])
    h["cache-used"] = used
    h["cache-total"] = total

    h["read-hits"] = int(tokens[4])
    h["read-misses"] = int(tokens[5])
    h["write-hits"] = int(tokens[6])
    h["write-misses"] = int(tokens[7])
    h["demotions"] = int(tokens[8])
    h["promotions"] = int(tokens[9])
    h["nr-dirty"] = int(tokens[10])

    pos = 11 + _parse_feature_flags(h, tokens[11:])
    pos += _parse_core_args(h, tokens[pos:])

    h["policy"] = _parse_policy_name(h, tokens[pos])
    pos += 1 + _parse_policy_config_values(h, tokens[pos + 1:])

    _parse_mode(h, tokens[pos])
    h["needs-check"] = _parse_needs_check(tokens[-1])

    return h


def cache_status(dev):
    return _parse_cache_status(dev.status())
