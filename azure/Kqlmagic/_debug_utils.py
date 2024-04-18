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

from typing import Any
import os

from .my_utils import get_env_var_bool


debug_mode = get_env_var_bool("KQLMAGIC_DEBUG", False)

if debug_mode:
    def debug_print(obj:Any)->None:
        print(f">>> debug >>> {obj}")
else:
    def debug_print(obj:Any)->None:
        return
