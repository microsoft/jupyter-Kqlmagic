#!/usr/bin/python
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

# Note: None as default is valid
# commands that have no parameters, typekey should not exit or set to None
# default max_params is 1 if type is not None otherwise 0
# default min_params is 1 if type is not None otherwise 0
COMMANDS_TABLE = {
    "version": {"flag": "version", "type": None},
    "banner": {"flag": "banner", "type": None},
    "usage": {"flag": "usage", "type": None},
    "submit": {"flag": "submit", "type": None, "cell": True},  # default
    "help": {"flag": "help", "type": "str", "default": "help"},
    "faq": {"flag": "faq", "type": None},
    "palette": {"flag": "palette", "type": None},
    "palettes": {"flag": "palettes", "type": None},
    # "config": {"flag": "config", "type": "str", "default": None},
    "config": {"flag": "config", "type": "not_quoted_str", "allow_none": True, "max_params": 1, "min_params": 0},
    "bugreport": {"flag": "bug_report", "type": None},
    "conn": {"flag": "conn", "type": "str", "default": None},
    # should be per connection
    "cache": {"flag": "cache", "type": "str", "allow_none": True},
    "cachecreate": {"flag": "cache_create", "type": "str", "allow_none": True},
    "cacheappend": {"flag": "cache_append", "type": "str", "allow_none": True},
    "cachecreateorappend": {"flag": "cache_create_or_append", "type": "str", "allow_none": True},
    "cacheremove": {"flag": "cache_remove", "type": "str", "allow_none": True},
    "cachelist": {"flag": "cache_list", "type": None},
    "cachestop": {"flag": "cache_stop", "type": None},

    "usecache": {"flag": "use_cache", "type": "str", "allow_none": True},
    "usecachestop": {"flag": "use_cache_stop", "type": "str", "allow_none": True},
    "schema": {"flag": "schema", "type": "str", "default": None},
    "clearssodb": {"flag": "clear_sso_db", "type": None},
    "py": {"flag": "python", "type": "not_quoted_str", "allow_none": True, "max_params": 2, "min_params": 2, "cell": True},
    "pyro": {"flag": "python", "type": "not_quoted_str", "allow_none": True, "max_params": 2, "min_params": 2, "cell": True},
    "pyrw": {"flag": "python", "type": "not_quoted_str", "allow_none": True, "max_params": 2, "min_params": 2, "cell": True},
    "activatekernel": {"flag": "activate_kernel", "type": None},
    "deactivatekernel": {"flag": "deactivate_kernel", "type": None},

    "linemagic": {"flag": "line_magic", "type": "not_quoted_str", "allow_none": True, "max_params": 2, "min_params": 2},
    "cellmagic": {"flag": "cell_magic", "type": "not_quoted_str", "allow_none": True, "max_params": 3, "min_params": 3, "cell": True},
}
