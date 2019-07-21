# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""A module that manage package version.
"""

VERSION = "0.1.100"

import sys
import requests
from Kqlmagic.constants import Constants
from Kqlmagic.help import MarkdownString


def execute_version_command():
    """ execute the version command.
    command just return a string with the version that will be displayed to the user

    Returns
    -------
    str
        A string with the current version
    """
    return MarkdownString("{0} version: {1}".format(Constants.MAGIC_PACKAGE_NAME, VERSION))

def get_pypi_latest_version(package_name: str) -> str:
    """ Retreives latest package version string for PyPI.

    Parameters
    ----------
    package_name : str
        Name of package as define in PyPI.

    Returns
    -------
    str or None
        The latest version string of package in PyPI, or None if fails to retrieve

    Raises
    ------
    RequestsException
        If request to PyPI fails.
    """

    #
    # submit request
    #

    api_url = "https://pypi.org/pypi/{0}/json".format(package_name)
    response = requests.get(api_url)

    #
    # handle response
    #

    response.raise_for_status()

    json_response = response.json()
    return json_response["info"]["version"]


def compare_version(other: str) -> int:
    """ Compares current VERSION to another version string.

    Parameters
    ----------
    other : str
        The other version to compare with, assume string "X.Y.Z" X,Y,Z integers


    Returns
    -------
    int
        1 if VERSION higher than other
        0 if VERSION equal to other
        1 if VERSION lower than other
    """

    v = VERSION.strip(".")
    o = other.strip(".")
    if v == o:
        return 0

    for idx, v_val in enumerate(v):
        if idx < len(o):
            o_val = o[idx]
            if o_val != v_val:
                v_int = to_int(v_val)
                o_int = to_int(o_val)
                # both are int, so they can be compared, and also be equal ('05' == '5')
                if o_int is not None and v_int is not None:
                    if v_int != o_int:
                        return -1 if v_int > o_int else 1
                # both are not int, compare is determined by lexical string compare
                elif o_int is None and v_int is None:
                    return -1 if v_val > o_val else 1
                # any value not int is interpreted as less than 0
                else:
                    return -1 if o_int is None else 1

    if len(o) == len(v):
        return 0
    elif len(o) > len(v):
        return 1 if any([to_int(o_val) is None or to_int(o_val) > 0 for o_val in o[len(v) :]]) else 0
    else:
        return -1 if any([to_int(v_val) is None or to_int(v_val) > 0 for v_val in v[len(o) :]]) else 0


def is_int(str_val: str) -> bool:
    """ Checks whether a string can be converted to int.

    Parameters
    ----------
    str_val : str
        A string to be checked.

    Returns
    -------
    bool
        True if can be converted to int, otherwise False
    """

    return not (len(str_val) == 0 or any([c not in "0123456789" for c in str_val]))


def to_int(str_val: str):
    """ Converts string to int if possible.
    
    Parameters
    ----------
    str_val : str
        A string to be converted.

    Returns
    -------
    int or None
        Converted integer if success, otherwise None
    """

    return int(str_val) if is_int(str_val) else None

def validate_required_python_version_running(minimal_required_version: str) -> None:
    """ Validate whether the running python version meets minimal required python version 
    
    Parameters
    ----------
    minimal_required_version : str
        Minimal required python version, in the following format: major.minor.micro

    Returns
    -------
    None

    Exceptions
    ----------
    Raise RunTime exception, if sys.version_info does not support attributes: major, minor, micro (old python versions)
    Raise RunTime exception, if running python version is lower than required python version 

    """
    try:
        parts = minimal_required_version.split(".")
        min_py_version = 1000000*int(parts[0]) + 1000*(int(parts[1]) if len(parts) > 1 else 0) + (int(parts[2]) if len(parts) > 2 else 0)
        running_py_version = 1000000*sys.version_info.major + 1000*sys.version_info.minor + sys.version_info.micro
        if running_py_version < min_py_version:
            raise RuntimeError("Kqlmagic requires python >= {0}, you use python {1}.{2}.{3}".format(
                Constants.MINIMAL_PYTHON_VERSION_REQUIRED, 
                sys.version_info.major, 
                sys.version_info.minor,
                sys.version_info.micro))
    except:
        raise RuntimeError("Kqlmagic requires python >= {0}, you use python {1}".format(
            Constants.MINIMAL_PYTHON_VERSION_REQUIRED, 
            sys.version))
