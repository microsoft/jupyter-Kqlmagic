#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# -------------------------------------------------------

"""Module containing bug report helper(s)."""

import json
import platform
import sys
import ssl


from .version import VERSION as kqlmagic_version


def _implementation_info() -> dict:
    """Return a dict with the Python implementation and version.

    Provide both the name and the version of the Python implementation
    currently running. For example, on CPython 2.7.5 it will return
    {'name': 'CPython', 'version': '2.7.5'}.
    """

    implementation = 'Unknown'
    implementation_version = 'Unknown'
    try:
        implementation = platform.python_implementation()

        if implementation == 'CPython':
            implementation_version = platform.python_version()

        elif implementation == 'PyPy':
            implementation_version = f'{sys.pypy_version_info.major}.{sys.pypy_version_info.minor}.{sys.pypy_version_info.micro}'
            
            if sys.pypy_version_info.releaselevel != 'final':
                implementation_version = ''.join([
                    implementation_version, sys.pypy_version_info.releaselevel
                ])

        else:
            implementation_version = 'Unknown'

    except:
        pass

    return {'name': implementation, 'version': implementation_version}


def _platform_info() -> dict:
    """Return a dict with the system version and release."""

    platform_system = 'Unknown'
    platform_release = 'Unknown'
    try:
        platform_system = platform.system()
        platform_release = platform.release()

    except:
        pass

    return {'system': platform_system, 'release': platform_release}


def bug_info():
    """Generate information for a bug report."""

    implementation_info = _implementation_info()
    platform_info = _platform_info()

    # TODO: collect information about: 
    #       jupyter information (front end, versions)
    #       ipython environment (version, temp file locations)
    #       all modules versions
    #       cell content, 
    #       environment variables, 
    #       default options, 
    #       result object (if doesn't exist), 
    #       last error (including stack), 

    return {
        'kqlmagic': {'version': kqlmagic_version},
        'platform': platform_info,
        'implementation': implementation_info,
    }


def bug_report():
    """Pretty-print the bug information as JSON."""

    # TODO: provide email address or forum for the bug report
    # this print is not for debug
    print(json.dumps(bug_info(), sort_keys=True, indent=4))

