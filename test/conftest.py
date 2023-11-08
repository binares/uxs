import pytest
import os
import copy
import yaml
import uxs.base._settings as _settings
from uxs.base._settings import (
    SETTINGS,
    _SETTINGS_INITIAL,
    APPDATA_DIR,
    read_settings,
    set_cache_dir,
)

test_dir = os.path.join(APPDATA_DIR, "test")


# Execute this once per module
# Fixtures defined in conftest.py can be accessed in any test function
# without importing (if it is included as an argument of the function)
@pytest.fixture(scope="module")
def init():
    _init()


def _init():
    cleanup()

    try:
        os.mkdir(test_dir)
    except FileExistsError:
        pass

    settings_test_path = init_settings_path()

    SETTINGS.update(copy.deepcopy(_SETTINGS_INITIAL))

    init_test_cache_dir()

    # Create the new settings_test.yaml
    read_settings(False)

    return test_dir, settings_test_path


def init_settings_path():
    _settings.SETTINGS_PATH = settings_test_path = os.path.join(
        test_dir, "settings_test.yaml"
    )

    # overwrite any previous content
    with open(settings_test_path, "w"):
        pass

    _safe_remove(settings_test_path)

    return settings_test_path


def init_test_cache_dir():
    # SETTINGS['CACHE_DIR'] = test_dir
    set_cache_dir(test_dir, True)

    # Messing with initial settings is unwise because pytest collects all test modules at once
    # and thus the _settings.py globals are shared across all tests modules
    # _SETTINGS_INITIAL['CACHE_DIR'] = test_dir

    return test_dir


def _safe_remove(pth):
    try:
        os.remove(pth)
    except OSError:
        pass


def cleanup():
    try:
        for root, dirs, files in os.walk(test_dir, topdown=False):
            for x in os.listdir(root):
                _safe_remove(os.path.join(root, x))
        for x in os.listdir(test_dir):
            _safe_remove(os.path.join(test_dir, x))
    except OSError:
        pass

    _safe_remove(test_dir)


# Execute this after all tests in all modules are finished
def pytest_sessionfinish(session, exitstatus):
    cleanup()
