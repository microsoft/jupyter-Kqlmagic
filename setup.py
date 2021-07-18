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


# Always prefer setuptools over distutils
from setuptools import setup, find_packages



#==============================================================================
# _IS_CUSTOM_SETUP VALUE IS MODIFIED BY A SHELL PREPROCESSOR SCRIPT
# TO FORK TWO PACKAGES:
#      - Kqlmagic - INCLUDES ALL DEPENDENCIES
#      - KqlmagicCustom - DEPENDENCIES NEED TO BE EXPLICITLY SPECIFIED
#==============================================================================
# NEXT CODE LINE SHOULD NOT BE TOUCHED/MODIFIED MANUALLY, MAKE SURE IT APPEARS ONLY ONCE
_IS_CUSTOM_SETUP = True # MODIFIED BY A SHELL PREPROCESSOR SCRIPT
#==============================================================================
#
#

_PACKAGE_DASHED_PATH   = 'azure-Kqlmagic'

_VERSION_CODE_FILENAME = '_version.py'
_REQUIRE_CODE_FILENAME = '_require.py'
_META_CODE_FILENAME    = '_meta.py'
_TEST_CODE_FILENAME    = '_test.py'


#
# help functions
#

# Union Without repetition  
def list_union(*args): 
    final_list = list(set().union(*args)) 
    return final_list 


def read(relative_path, encoding='utf-8'):
    path = os.path.join(os.path.dirname(__file__), relative_path)
    with io.open(path, encoding=encoding) as fp:
        return fp.read()


def write(content, relative_path, encoding='utf-8'):
    path = os.path.join(os.path.dirname(__file__), relative_path)
    with io.open(path, mode="w", encoding=encoding) as fp:
        return fp.write(content)


def exec_py_code(filename):
    """Get requires from _require.py file."""

    code_relative_path = _PACKAGE_DASHED_PATH.replace('-', '/') + '/' + filename
    try:
        package_relative_path = _PACKAGE_DASHED_PATH.replace('-', os.path.sep)

        code_relative_path = os.path.join(package_relative_path, filename)
        py_code = read(code_relative_path)
        user_ns = {}
        exec(py_code, {}, user_ns)
        return user_ns
    except Exception as error:
        raise RuntimeError("Failed to execute py code from file: {} due to error: {}".format(code_relative_path, error))


#
# get version
#

def get_version():
    """Get version and devstatus from _version.py file."""
    user_ns = exec_py_code(_VERSION_CODE_FILENAME)
    # Get development Status for classifiers
    version = user_ns["__version__"]
    version_info = user_ns["__version_info__"]
    devstatus = {
        'dev':   '2 - Pre-Alpha',
        'alpha': '3 - Alpha',
        'beta':  '4 - Beta',
        'rc':    '4 - Beta',
        'final': '5 - Production/Stable'
    }[version_info[3]]
    return version, devstatus

VERSION, _DEVSTATUS = get_version()


#
# get package metadata
#

def get_meta(is_custom_setup, devstatus):
    """Get metadata from _meta.py file."""
    meta = exec_py_code(_META_CODE_FILENAME)
    if is_custom_setup:
        meta["NAME"] = meta["NAME"] + "Custom"
    meta["CLASSIFIERS"].insert(0, 'Development Status :: {}'.format(devstatus))
    return meta

meta = get_meta(_IS_CUSTOM_SETUP, _DEVSTATUS)

DESCRIPTION         = meta["DESCRIPTION"]
NAME                = meta['NAME']
AUTHOR              = meta["AUTHOR"]
AUTHOR_EMAIL        = meta["AUTHOR_EMAIL"]
MAINTAINER          = meta["MAINTAINER"]
MAINTAINER_EMAIL    = meta["MAINTAINER_EMAIL"]
URL                 = meta["URL"]
PROJECT_URLS        = meta["PROJECT_URLS"]
LICENSE             = meta["LICENSE"]
KEYWORDS            = meta["KEYWORDS"]
CLASSIFIERS         = meta["CLASSIFIERS"]
PYTHON_REQUIRES     = meta["PYTHON_REQUIRES"]


#
# check minimal sanity
#

#==============================================================================
# Minimal Python version sanity check
# Taken from the notebook setup.py -- Modified BSD License
#==============================================================================
def minimal_sanity(python_requires, kqlmagic_name, kqlmagic_version):

    def get_version_tuple(item:str):
        "parse package name from a package line that include version info"
        ver_tuple = (3, 6)
        item = item.strip().lower()
        for i in range(len(item)):
            if item[i].isdigit():
                ver_tuple = [int(val) for val in item[i:].split(".")]
                break
        return ver_tuple

    v = sys.version_info
    min_python_required_ver = get_version_tuple(python_requires)
    if list(v[:2]) < min_python_required_ver:
        pip_message = 'This may be due to an out of date pip. Make sure you have pip >= 9.0.1.'
        try:
            import pip
            pip_version = tuple([int(x) for x in pip.__version__.split('.')[:3]])
            if pip_version < (9, 0, 1) :
                pip_message = 'Your pip version is out of date, please install pip >= 9.0.1. '\
                'pip {} detected.'.format(pip.__version__)
            else:
                # pip is new enough - it must be something else
                pip_message = ''
        except Exception:
            pass

        error = """ERROR: {name} {ver} requires Python version 3.6 or above.

        Python {py} detected.
        {pip}
        """.format(name=kqlmagic_name, ver=kqlmagic_version, py=sys.version_info, pip=pip_message)
        print(error, file=sys.stderr)
        sys.exit(1)

minimal_sanity(PYTHON_REQUIRES, NAME, VERSION)


#
# get long description
#

def get_long_description():
    """Get long description from REAM.RST and NEWS.txt."""
    try:
        readme_rst_content = read('README.rst')
        long_description_content_type = 'text/x-rst' # long_description_content_type = 'text/markdown'
        news_txt_content = read('NEWS.txt')
        long_description =  (readme_rst_content + '\n\n' + news_txt_content).replace('\r','')
        return long_description, long_description_content_type
    except Exception as error:
        raise RuntimeError("Unable to README.rst or NEWS.txt file, due to error: {}".format(error))

LONG_DESCRIPTION, LONG_DESCRIPTION_CONTENT_TYPE = get_long_description()


#
# get required dependencies
#

def get_require():
    user_ns = exec_py_code(_REQUIRE_CODE_FILENAME)
    return user_ns["INSTALL_REQUIRES"], user_ns["EXTRAS_REQUIRE"], user_ns["DEV_REQUIRES"], user_ns["TESTS_REQUIRE"] # pylint: disable=no-member

INSTALL_REQUIRES, EXTRAS_REQUIRE, DEV_REQUIRES, TESTS_REQUIRE = get_require()

if _IS_CUSTOM_SETUP:
    DESCRIPTION = DESCRIPTION + " (Custom Dependencies)"
else:
    INSTALL_REQUIRES = list_union(INSTALL_REQUIRES, EXTRAS_REQUIRE.get("default"))
    EXTRAS_REQUIRE   = {}

EXTRAS_REQUIRE["dev"] = DEV_REQUIRES
EXTRAS_REQUIRE["tests"] = TESTS_REQUIRE


#
# get test command class
#

def get_test_class():
    user_ns = exec_py_code(_TEST_CODE_FILENAME)
    return user_ns["TestClass"] # pylint: disable=no-member

TEST_CLASS = get_test_class()


def kwargs_params_to_dict(**kwargs):
    import json
    print(json.dumps(kwargs, default=lambda o: str(o), indent=4))
    return kwargs

setup_kwargs = kwargs_params_to_dict(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type=LONG_DESCRIPTION_CONTENT_TYPE,
    classifiers=CLASSIFIERS,
    keywords=KEYWORDS,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    maintainer=MAINTAINER,
    maintainer_email=MAINTAINER_EMAIL,
    url=URL,
    license=LICENSE,
    python_requires=PYTHON_REQUIRES,
    packages=find_packages('azure'),
    package_dir={'': 'azure'},
    include_package_data=True,
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    project_urls=PROJECT_URLS,
    cmdclass={'test': TEST_CLASS},
    tests_require=TESTS_REQUIRE,
)

setup(**setup_kwargs)
