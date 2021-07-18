# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""A module to execute that allows %%kql be one line only
"""

from typing import List, Dict


from traitlets.config.configurable import Configurable


from ._debug_utils import debug_print
from .constants import Constants
from .ipython_api import IPythonAPI


class AllowSingleLineCell(object):

    config:Configurable = None
    allow_single_line_cell_default = False

    @staticmethod
    def initialize(config:Configurable):
        AllowSingleLineCell.config = config
        AllowSingleLineCell.allow_single_line_cell_default = config.allow_single_line_cell
        if config.is_magic is True:
            AllowSingleLineCell.allow_single_line_cell(config.allow_single_line_cell)
        else:
            config.set_trait("allow_single_line_cell", False, force=True)
        
        config.observe(AllowSingleLineCell.observe_allow_single_line_cell, names=["allow_single_line_cell"])
        config.observe(AllowSingleLineCell.observe_is_magic, names=["is_magic"])


    @staticmethod
    def allow_single_line_cell(to_activate:bool)->bool:
        to_add_input_transformer = to_activate
        if to_add_input_transformer:
            IPythonAPI.try_add_input_transformers_cleanup(AllowSingleLineCell.add_empty_line_suffix)
        else:
            IPythonAPI.try_remove_input_transformers_cleanup(AllowSingleLineCell.add_empty_line_suffix)
        return IPythonAPI.is_in_input_transformers_cleanup(AllowSingleLineCell.add_empty_line_suffix)


    @staticmethod
    def add_empty_line_suffix(lines:List[str])->List[str]:
        new_lines = lines
        if len(lines) == 1:
            first_line = lines[0]
            cell_magic_name = first_line.split(None, 1)[0]
            if cell_magic_name in [Constants.CELL_MAGIC_PREFIX]:
                cell_magic_name = first_line.split(None, 1)[0]
                if not first_line.endswith("\n"):
                    first_line = first_line + "\n"
                new_lines = [first_line, "\n"]
        return new_lines


    @staticmethod
    def observe_allow_single_line_cell(change:Dict[str,str]):
        if change.get('type') == 'change' and change.get('name') == 'allow_single_line_cell':
            if AllowSingleLineCell.config.is_magic is False:
                if change.get('new') is True:
                    AllowSingleLineCell.config.set_trait('allow_single_line_cell', False, force=True)
            else:
                AllowSingleLineCell.allow_single_line_cell(change.get('new'))


    @staticmethod
    def observe_is_magic(change:Dict[str,str]):
        if change.get('type') == 'change' and change.get('name') == 'is_magic':
            allow_single_line_cell = change.get('new') and AllowSingleLineCell.config.allow_single_line_cell
            AllowSingleLineCell.config.set_trait('allow_single_line_cell', allow_single_line_cell, force=True)
            AllowSingleLineCell.allow_single_line_cell(allow_single_line_cell)
