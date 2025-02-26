# Installation

- Make sure python3 is installed.
- Make sure pip is installed.
- Install python deps:

```bash
pip install -r requirements.txt
```

- Edit config.toml for your system
- Check for tool requirements by running:

```bash
./dmtest health
```

In addition, many of the tests perform operations on a copy of
linux git repository:

```bash
git clone https://github.com/torvalds/linux.git
```

The kernel source code needs to be located in the dmtest-python directory.  You can override this location
using the environment variable `DMTEST_KERNEL_SOURCE`, e.g. `DMTEST_KERNEL_SOURCE=/home/someuser/linux`

# Running

Many operations require the option `--result-set <some arbitrary name>` to function.  This can be supplied by using
the environment variable `DMTEST_RESULT_SET`

```bash
export DMTEST_RESULT_SET=baseline
```

## List tests

```bash
export DMTEST_RESULT_SET=baseline
./dmtest list --rx <regex>
```

## Run tests

```bash
./dmtest run --rx <regex>
```

## List test logs

```bash
./dmtest log 
```
