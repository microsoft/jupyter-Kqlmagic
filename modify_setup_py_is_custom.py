#!/usr/bin/env python

#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------
"""Setup for Kqlmagic"""


import sys
import os
import io

COLOR_RESET = "\033[0m"
COLOR_RED   = "\033[31m"
COLOR_GREEN = "\033[32m"

_SETUP_PY_FILENAME = 'setup.py'


def read(relative_path, encoding='utf-8'):
    path = os.path.join(os.path.dirname(__file__), relative_path)
    with io.open(path, encoding=encoding) as fp:
        return fp.read()

def write(content, relative_path, encoding='utf-8'):
    path = os.path.join(os.path.dirname(__file__), relative_path)
    with io.open(path, mode="w", encoding=encoding) as fp:
        return fp.write(content)


def modify_setup_py_is_custom(value):
    pattern = """_IS_CUSTOM_SETUP = {} # MODIFIED BY A SHELL PREPROCESSOR SCRIPT"""

    setup_py_code = read(_SETUP_PY_FILENAME)
    current = pattern.format(not value)
    new = pattern.format(value)
    if setup_py_code.find(current) >= 0:
        setup_py_code = setup_py_code.replace(current, new, 1)
        setup_py_code = write(setup_py_code, _SETUP_PY_FILENAME)
    elif setup_py_code.find(new) < 0:
        raise Exception(f"didn't find pattern in code. patter: {pattern}")

    

if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            print(F"{COLOR_RED}failed to modify setup.py, missing bool True/False argument{COLOR_RESET}")
            sys.exit(1)
        value = sys.argv[1].lower()
        if value not in ['true', 'false']:
            print(F"{COLOR_RED}failed to modify setup.py, invalid bool argument, should be either True or False{COLOR_RESET}")
            sys.exit(1)
        value = value == 'true'
        modify_setup_py_is_custom(value)
        print(f"{COLOR_GREEN}setup.py was modified, _IS_CUSTOM_SETUP = {value}{COLOR_RESET}")
    except Exception as error:
        print(f"{COLOR_RED}failed to modify setup.py, due to error: {error}{COLOR_RESET}")
        sys.exit(1)
