#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# because it is also executed from setup.py, make sure
# that it imports only modules, that for sure will exist at setup.py execution
# TRY TO KEEP IMPORTS TO MINIMUM
# --------------------------------------------------------------------------

# Union Without repetition  
def list_union(*args): 
    final_list = list(set().union(*args)) 
    return final_list

def strip_package_name(item):
    "parse package name from a package line that include version info"
    item = item.strip().lower()
    for i in range(len(item)):
        if item[i] in ' <>=~!;':
           return item[:i]
    return item

# ---------------------------------------------------
_IPYTHON_REQUIRES    = [
    'ipython>=7.1.1',
]

_JUPYTER_REQUIRES    = [ 
    'ipykernel>=5.1.1',
]
# ---------------------------------------------------


# ---------------------------------------------------
_ONLINE_REQUIRES     = [ 
    'requests>=2.21.0',
]

_AUTH_REQUIRES = [
    'msal>=1.11.0',
]

_PRETTY_TABLE_RQUIRES = [ 
    'prettytable>=0.7.2',    
]

_BASIC_REQUIRES = list_union(_ONLINE_REQUIRES, _AUTH_REQUIRES, _PRETTY_TABLE_RQUIRES)
# ---------------------------------------------------


# ---------------------------------------------------
_BASE_UTILS_REQUIRES = [
    'psutil>=5.4.7',
    'setuptools>=41.0.1', # optional  
    'flask>=1.0.3', # must have for Azure Data Studio SAW
    'isodate>=0.6.0',
]

_TEXT_UTILS_REQUIRES = [
    'Markdown>=3.0.1',
    'beautifulsoup4>=4.6.3',
    'lxml>=4.2.5',
]

_JSON_COLOR_REQUIRES = [
    'Pygments>=2.2.0',
]

_AUTH_CODE_TO_CLIPBOARD_REQUIRES = [
    'pyperclip>=1.7.0',
]

_MATPLOTLIB_PALETTES_REQUIRES = [
    'matplotlib>=3.0.0',
]

_UTILS_REQUIRES = list_union(_BASE_UTILS_REQUIRES, _TEXT_UTILS_REQUIRES, _JSON_COLOR_REQUIRES, _AUTH_CODE_TO_CLIPBOARD_REQUIRES,
                               _MATPLOTLIB_PALETTES_REQUIRES)
# ---------------------------------------------------


# ---------------------------------------------------
_PLOT_PLOTLY_REQUIRES = [
    'plotly>=3.10.0',
]

_PLOT_REQUIRES = list_union(_PLOT_PLOTLY_REQUIRES)

_DATAFRAME_PANDAS_REQUIRES = [
    'pandas>=0.23.4',
]

_DATAFRAME_REQUIRES = list_union(_DATAFRAME_PANDAS_REQUIRES)

_IPYWIDGETS_REQUIRES = [
    'ipywidgets',
]

_KQLMAGIC_FERNET_SSO_REQUIRES = [
    'cryptography>=2.7', # also required by adal/msal
    'password-strength>=0.0.3',
]

_KQLMAGIC_MSAL_SSO_REQUIRES = [
    'msal-extensions>=0.3.0', # required for SSO
]


_KQLMAGIC_SSO_REQUIRES = list_union(_AUTH_REQUIRES, _KQLMAGIC_MSAL_SSO_REQUIRES, _KQLMAGIC_FERNET_SSO_REQUIRES)

_AZCLI_SSO_REQUIRES = [
    'azure-identity>=1.5.0',
    'azure-common>=1.1.25',
]

_VSCODE_SSO_REQUIRES = [
    'azure-identity>=1.5.0',
]

_MSAL_SSO_REQUIRES = [
    'msal-extensions>=0.3.0', # required for SSO
]

_MSI_SSO_REQUIRES = [
    'msrestazure>=0.6.3',
]

_SSO_REQUIRES = list_union(_KQLMAGIC_SSO_REQUIRES, _AZCLI_SSO_REQUIRES, _VSCODE_SSO_REQUIRES, _MSAL_SSO_REQUIRES,
                          _MSI_SSO_REQUIRES)

_EXTRA_REQUIRES = list_union(_PLOT_REQUIRES, _DATAFRAME_REQUIRES, _SSO_REQUIRES, _IPYWIDGETS_REQUIRES)
# ---------------------------------------------------

_SAW_REQUIRES = list_union(_JUPYTER_REQUIRES, _IPYTHON_REQUIRES, _IPYWIDGETS_REQUIRES, _DATAFRAME_PANDAS_REQUIRES, _AUTH_REQUIRES, _JSON_COLOR_REQUIRES)


# the most slim configuration
_NAKED_REQUIRES = []

# includes all configuration

_PYTHON_BASIC_REQUIRES  = list_union(_BASIC_REQUIRES)
_JUPYTER_BASIC_REQUIRES = list_union(_PYTHON_BASIC_REQUIRES, _JUPYTER_REQUIRES)
_IPYTHON_BASIC_REQUIRES = list_union(_PYTHON_BASIC_REQUIRES, _IPYTHON_REQUIRES)
_BASIC_REQUIRES         = list_union(_PYTHON_BASIC_REQUIRES, _JUPYTER_REQUIRES, _IPYTHON_REQUIRES)

_PYTHON_EXTENDED_REQUIRES  = list_union(_PYTHON_BASIC_REQUIRES, _UTILS_REQUIRES, _SSO_REQUIRES)
_JUPYTER_EXTENDED_REQUIRES = list_union(_PYTHON_EXTENDED_REQUIRES, _JUPYTER_REQUIRES)
_IPYTHON_EXTENDED_REQUIRES = list_union(_PYTHON_EXTENDED_REQUIRES, _IPYTHON_REQUIRES)
_EXTENDED_REQUIRES         = list_union(_PYTHON_EXTENDED_REQUIRES, _JUPYTER_REQUIRES, _IPYTHON_REQUIRES)

_PYTHON_ALL_REQUIRES  = list_union(_PYTHON_EXTENDED_REQUIRES, _EXTRA_REQUIRES)
_JUPYTER_ALL_REQUIRES = list_union(_PYTHON_ALL_REQUIRES, _JUPYTER_REQUIRES)
_IPYTHON_ALL_REQUIRES = list_union(_PYTHON_ALL_REQUIRES, _IPYTHON_REQUIRES)
_ALL_REQUIRES         = list_union(_PYTHON_ALL_REQUIRES, _JUPYTER_REQUIRES, _IPYTHON_REQUIRES)

_DEFAULT_REQUIRES = _ALL_REQUIRES

# ---------------------------------------------------
# ---------------------------------------------------
# ---------------------------------------------------
INSTALL_REQUIRES  = [
    'python-dateutil>=2.7.5', # also required by adal/msal, pandas
    'traitlets>=4.3.2', # must have, basic
]

EXTRAS_REQUIRE      = {
    'default': _DEFAULT_REQUIRES,

    'naked': _NAKED_REQUIRES,

    'saw': _SAW_REQUIRES,

    'jupyter-basic': _JUPYTER_BASIC_REQUIRES,
    'ipython-basic': _IPYTHON_BASIC_REQUIRES,
    'python-basic': _PYTHON_BASIC_REQUIRES,
    'basic': _BASIC_REQUIRES,

    'jupyter-extended': _JUPYTER_EXTENDED_REQUIRES,
    'ipython-extended': _IPYTHON_EXTENDED_REQUIRES,
    'python-extended': _PYTHON_EXTENDED_REQUIRES,
    'extended': _EXTENDED_REQUIRES,

    'jupyter-all': _JUPYTER_ALL_REQUIRES,
    'ipython-all': _IPYTHON_ALL_REQUIRES,
    'python-all': _PYTHON_ALL_REQUIRES,

    'plotly': _PLOT_PLOTLY_REQUIRES,
    'pandas': _DATAFRAME_PANDAS_REQUIRES,
    'widgets': _IPYWIDGETS_REQUIRES,

    'sso': _SSO_REQUIRES,
    'azcli_sso': _AZCLI_SSO_REQUIRES,
    'msi_sso': _MSI_SSO_REQUIRES,
    'vscode_sso': _VSCODE_SSO_REQUIRES,
    'msal_sso': _MSAL_SSO_REQUIRES,
    'kqlmagic_sso': _KQLMAGIC_SSO_REQUIRES,
    'kqlmagic_msal_sso': _KQLMAGIC_FERNET_SSO_REQUIRES,
    'kqlmagic_fernet_sso': _KQLMAGIC_FERNET_SSO_REQUIRES,

    'utils': _UTILS_REQUIRES,
    'base_utils': _BASE_UTILS_REQUIRES,
    'text_utils': _TEXT_UTILS_REQUIRES,
    'json_color': _JSON_COLOR_REQUIRES,
    'auth_code_clipboard': _AUTH_CODE_TO_CLIPBOARD_REQUIRES,
    'matplotlib_palettes': _MATPLOTLIB_PALETTES_REQUIRES,
}

TESTS_REQUIRE       = [
    'pytest',
    'pytest-pep8',
    'pytest-docstyle',
    'pytest-flakes',
    'pytest-cov',

    'QtPy',
    'PyQt5'
]

DEV_REQUIRES       = [
    'twine',      # Collection of utilities for publishing packages on PyPI
    'pip',        # The PyPA recommended tool for installing Python packages
    'wheel',      # A built-package format for Python
    'black',      # code formatter

    'flake8',     # analyzes programs and detects various errors
    'pipreqs',    # Pip requirements.txt generator based on imports in project

    'mccabe',     # Ned’s script to check McCabe complexity, plugin for flake8
    'pep8'        # heck PEP-8 naming conventions, plugin for flake8
    'autopep8',   # A tool that automatically formats Python code to conform to the PEP 8 style guide
    'pydocstyle', # tatic analysis tool for checking compliance with Python docstring conventions
]
