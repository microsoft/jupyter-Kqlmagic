#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# -------------------------------------------------------

"""Module containing bug report helper(s)."""

import platform
import sys


from ._version import __version__ as kqlmagic_version
from .dependencies import Dependencies



def _python_info() -> dict:
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
            if hasattr(sys, "pypy_version_info"):
                pypy_version_info = getattr(sys, "pypy_version_info")
                implementation_version = f'{pypy_version_info.major}.{pypy_version_info.minor}.{pypy_version_info.micro}'
                
                if pypy_version_info.releaselevel != 'final':
                    implementation_version = ''.join([
                        implementation_version, pypy_version_info.releaselevel
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


def _packages_info() -> dict:
    """Return a dict with installed packages version"""

    return Dependencies.installed_packages()


def bug_info(default_options, default_env, connections_info, last_execution:dict):
    """Generate information for a bug report."""

    python_info = _python_info()
    platform_info = _platform_info()
    packages_info = _packages_info()
    default_options_info = default_options
    last_execution = last_execution

    # TODO: collect information about: 
    #       jupyter information (front end, versions)
    #       ipython environment (version, temp file locations)
    #       all modules versions
    #       cell content, 
    #       environment variables, (that starts with KQLMAGIC and others)
    #       default options, 
    #       result object (if doesn't exist), 
    #       last error (including stack), 

    return {
        'kqlmagic': {'version': kqlmagic_version},
        'platform': platform_info,
        'packages': packages_info,
        'python': python_info,
        'kqlmagic_default_options': default_options_info,
        'kqlmagic_connections:': connections_info,
        'kqlmagic_default_env': default_env,
        'Kqlmagic_last_execution': last_execution,
    }
