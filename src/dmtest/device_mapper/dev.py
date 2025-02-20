from contextlib import contextmanager
import random

import dmtest.device_mapper.interface as dm


class Dev:
    def __init__(self, name):
        self._name = name
        self._path = f"/dev/mapper/{name}"
        self._active_table = None
        dm.create(self._name)

    def __str__(self):
        return self._path

    # for PathLike protocol, e.g., open(some_dev,...)
    def __fspath__(self):
        return self._path

    @property
    def name(self):
        return self._name

    @property
    def path(self):
        return self._path

    def load(self, table):
        self._active_table = table
        dm.load(self._name, table)

    def load_ro(self, table):
        self._active_table = table
        dm.load_ro(self._name, table)

    def suspend(self):
        dm.suspend(self._name)

    def suspend_noflush(self):
        dm.suspend_noflush(self._name)

    def resume(self):
        dm.resume(self._name)

    def remove(self):
        dm.remove(self._name)
        if self._active_table is not None:
            for target in self._active_table:
                target.post_remove_check()

    def message(self, sector, *args):
        return dm.message(self._name, sector, *args)

    def status(self, noflush=False):
        if noflush:
            return dm.status(self._name, "--noflush")
        else:
            return dm.status(self._name)

    def table(self):
        return dm.table(self._name)

    def info(self):
        return dm.info(self._name)

    def wait(self, event_nr):
        dm.wait(self._name, event_nr)

    def event_nr(self):
        output = dm.status(self._name, "-v")
        dm.parse_event_nr(output)

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self.remove()

    @contextmanager
    def pause(dev, noflush=False):
        try:
            if noflush:
                dev.suspend_noflush()
            else:
                dev.suspend()
            yield dev
        finally:
            dev.resume()


def random_name():
    return f"test-dev-{random.randint(0, 1000000)}"


def dev(table, read_only=False):
    """
    A context manager for creating, using, and automatically cleaning up a device-mapper device.

    This context manager creates a device-mapper device with the specified table and
    read-only status, activates it, and yields the device. Once the context is exited,
    it automatically removes the device.

    Note: context manager functionality has been moved to class Dev to allow users to call this function without
    requiring the "with" usage.

    Args:
        table (str): The device-mapper table string to be used for creating the device.
        read_only (bool, optional): If True, the device will be loaded with read-only status.
                                     Defaults to False.

    Yields:
        Dev: The created and activated device-mapper device instance.
    """
    name = random_name()

    dev = Dev(name)
    try:
        if read_only:
            dev.load_ro(table)
        else:
            dev.load(table)
        dev.resume()
    except Exception as e:
        dev.remove()
        raise e
    return dev


class DeviceCleanupError(Exception):
    def __init__(self, errors):
        super().__init__("Errors occurred during device cleanup")
        self.errors = errors


@contextmanager
def devs(*tables):
    """
    Creates one or more anonymous device-mapper devices and yields a tuple of
    the created devices.
    Args:
        tables (list): A tuple of table strings, one for each device to
                        create.
    Yields:
        list: A tuple of the created device-mapper devices.
    Raises:
        Exception: If any device-mapper devices fail to create.
        DeviceCleanupError: If any device-mapper devices fail to remove.
    """
    dev_instances = []

    try:
        # Create devices
        for table in tables:
            name = random_name()
            dev_instance = Dev(name)
            dev_instance.load(table)
            dev_instance.resume()
            dev_instances.append(dev_instance)

        yield tuple(dev_instances)

    finally:
        # Remove devices and handle exceptions
        cleanup_errors = []

        for dev_instance in dev_instances:
            try:
                dev_instance.remove()
            except Exception as e:
                cleanup_errors.append(e)

        if cleanup_errors:
            raise DeviceCleanupError(cleanup_errors)
