# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""A module that allows python comment '#" to prefix %%kql cell
"""

from typing import List, Dict


from traitlets.config.configurable import Configurable


from ._debug_utils import debug_print
from .constants import Constants
from .ipython_api import IPythonAPI


class AllowPyCommentsBeforeCell(object):

    config:Configurable = None
    allow_py_comments_before_cell_default = False

    @staticmethod
    def initialize(config:Configurable):
        AllowPyCommentsBeforeCell.config = config
        AllowPyCommentsBeforeCell.allow_py_comments_before_cell_default = config.allow_py_comments_before_cell
        if config.is_magic is True:
            AllowPyCommentsBeforeCell.allow_py_Comments_before_cell(config.allow_py_comments_before_cell)
        else:
            config.set_trait("allow_py_comments_before_cell", False, force=True)
        
        config.observe(AllowPyCommentsBeforeCell.observe_allow_py_comments_before_cell, names=["allow_py_comments_before_cell"])
        config.observe(AllowPyCommentsBeforeCell.observe_is_magic, names=["is_magic"])


    @staticmethod
    def allow_py_Comments_before_cell(to_activate:bool)->bool:
        to_add_input_transformer = to_activate
        if to_add_input_transformer:
            IPythonAPI.try_add_input_transformers_cleanup(AllowPyCommentsBeforeCell.remove_py_Comments_before_cell)
        else:
            IPythonAPI.try_remove_input_transformers_cleanup(AllowPyCommentsBeforeCell.remove_py_Comments_before_cell)
        return IPythonAPI.is_in_input_transformers_cleanup(AllowPyCommentsBeforeCell.remove_py_Comments_before_cell)


    @staticmethod
    def remove_py_Comments_before_cell(lines:List[str])->List[str]:
        new_lines = lines
        skip_idx = 0
        for line in lines:
            if line.isspace():
                skip_idx += 1
            else:
                first_line = line.lstrip()
                if first_line.startswith(Constants.PYTHON_COMMENT_PREFIX):
                    skip_idx += 1
                else:
                    if first_line.startswith(Constants.CELL_MAGIC_PREFIX):
                        skip_idx += 1
                        new_lines = lines[skip_idx:]
                        new_lines.insert(0, first_line)
                    break
        return new_lines


    @staticmethod
    def observe_allow_py_comments_before_cell(change:Dict[str,str]):
        if change.get('type') == 'change' and change.get('name') == 'allow_py_comments_before_cell':
            if AllowPyCommentsBeforeCell.config.is_magic is False:
                if change.get('new') is True:
                    AllowPyCommentsBeforeCell.config.set_trait('allow_py_comments_before_cell', False, force=True)
            else:
                AllowPyCommentsBeforeCell.allow_py_Comments_before_cell(change.get('new'))


    @staticmethod
    def observe_is_magic(change:Dict[str,str]):
        if change.get('type') == 'change' and change.get('name') == 'is_magic':
            allow_py_comments_before_cell = change.get('new') and AllowPyCommentsBeforeCell.config.allow_py_comments_before_cell
            AllowPyCommentsBeforeCell.config.set_trait('allow_py_comments_before_cell', allow_py_comments_before_cell, force=True)
            AllowPyCommentsBeforeCell.allow_py_Comments_before_cell(allow_py_comments_before_cell)
