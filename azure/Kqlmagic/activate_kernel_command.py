# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""A module to execute (user) %%kql cell python code
"""

from typing import List, Any, Dict
import ast
import tokenize

from traitlets.config.configurable import Configurable

from ._debug_utils import debug_print
from .constants import Constants
from .ipython_api import IPythonAPI
from .display import Display


class ActivateKernelCommand(object):

    config:Configurable = None
    is_kqlmagic_kernel_default:bool = False

    @staticmethod
    def initialize(config:Configurable):
        ActivateKernelCommand.config = config
        ActivateKernelCommand.is_kqlmagic_kernel_default = config.kqlmagic_kernel
        if config.is_magic is True:
            if config.kqlmagic_kernel:
                ActivateKernelCommand.execute(True)
        else:
            config.set_trait("kqlmagic_kernel", False, force=True)
        config.observe(ActivateKernelCommand.observe_is_magic, names=["is_magic"])


    @staticmethod
    def execute(to_activate:bool, options={}):
        ActivateKernelCommand.suppress_results = options.get("suppress_results")
        if ActivateKernelCommand.config.is_magic:
            is_kqlmagic_kernel = ActivateKernelCommand._activate_kernel(to_activate)
            ActivateKernelCommand.config.set_trait("kqlmagic_kernel", is_kqlmagic_kernel, force=True)
            if is_kqlmagic_kernel == to_activate:
                if not options.get("suppress_results"):
                    msg_title = {
                        "message": f"""{Constants.MAGIC_CLASS_NAME_LOWER} kernel {'' if to_activate else 'de'}activated""",
                        "style_options": {
                            "font-size":"30px"
                        }
                    }
                    Display.showInfoMessage(msg_title)
                    if to_activate:
                        Display.showInfoMessage([
                            '- to deactivate submit "--deactivate_kernel" command',
                            f'- to execute python code use "{Constants.PYTHON_CELL_MAGIC_PREFIX}" cell magic',
                            '- all python kernel cell and line magics are supported'])            
            else:
                Display.showDangerMessage(
                    f"Failed to {'' if to_activate else 'de'}activate {Constants.MAGIC_CLASS_NAME_LOWER} kernel"
                )
        else:
            ActivateKernelCommand.config.set_trait("kqlmagic_kernel", False, force=True)
            Display.showDangerMessage(
                f"{Constants.MAGIC_CLASS_NAME_LOWER} kernel is support only when loaded as a magic"
            )
            

    @staticmethod
    def _activate_kernel(to_activate:bool)->bool:
        if to_activate:
            IPythonAPI.try_add_input_transformers_cleanup(ActivateKernelCommand.add_kqlmagic_cell_prefix)
        else:
            IPythonAPI.try_remove_input_transformers_cleanup(ActivateKernelCommand.add_kqlmagic_cell_prefix)

        return IPythonAPI.is_in_input_transformers_cleanup(ActivateKernelCommand.add_kqlmagic_cell_prefix)


    @staticmethod
    def add_kqlmagic_cell_prefix(lines:List[str])->List[str]:
        # dont assume cleanup happens before
        insert_prefix = None
        skip_idx = 0
        for line in lines:
            if line.isspace():
                skip_idx += 1
            else:
                first_line = line.lstrip()
                if first_line.startswith(Constants.DONT_ADD_CELL_MAGIC_PREFIX):
                    skip_idx += 1
                elif first_line.startswith(Constants.IPYKERNEL_CELL_MAGIC_PREFIX):
                    cell_magic_name = first_line.split(None, 1)[0]
                    if cell_magic_name == Constants.PYTHON_CELL_MAGIC_PREFIX:
                        skip_idx += 1
                    else:
                        skip_idx = 0           
                else:
                    insert_prefix = Constants.CELL_MAGIC_PREFIX + "\n"
                break
        new_lines = lines[skip_idx:]
        if insert_prefix:
            new_lines.insert(0, insert_prefix)
        return new_lines


    @staticmethod
    def observe_is_magic(change:Dict[str,str]):
        if change.get('type') == 'change' and change.get('name') == 'is_magic':
            if change.get('new') is False:
                ActivateKernelCommand.config.set_trait("kqlmagic_kernel", False, force=True)             
                ActivateKernelCommand._activate_kernel(False)
            elif change.get('new') is True and ActivateKernelCommand.is_kqlmagic_kernel_default is True:
                ActivateKernelCommand.execute(True)


def is_python_code(lines:List[str])->bool:
    is_py_code = False
    try:
        py_code = "".join(lines)
        tr_py_code = IPythonAPI.transform_cell(py_code)
        ast.parse(tr_py_code)
        is_py_code = True
    except:
        pass
    return is_py_code





def make_tokens_by_line(lines:List[str])->List[List[tokenize.TokenInfo]]:
    """Tokenize a series of lines and group tokens by line.
    The tokens for a multiline Python string or expression are grouped as one
    line. All lines except the last lines should keep their line ending ('\\n',
    '\\r\\n') for this to properly work. Use `.splitlines(keeplineending=True)`
    for example when passing block of text to this function.
    """
    # NL tokens are used inside multiline expressions, but also after blank
    # lines or comments. This is intentional - see https://bugs.python.org/issue17061
    # We want to group the former case together but split the latter, so we
    # track parentheses level, similar to the internals of tokenize.

    #   reexported from token on 3.7+
    NEWLINE, NL = tokenize.NEWLINE, tokenize.NL  # type: ignore
    tokens_by_line:List[List[Any]] = [[]]
    if len(lines) > 1 and not lines[0].endswith(('\n', '\r', '\r\n', '\x0b', '\x0c')):
        pass
        # warnings.warn("`make_tokens_by_line` received a list of lines which do not have lineending markers ('\\n', '\\r', '\\r\\n', '\\x0b', '\\x0c'), behavior will be unspecified")
    parenlev = 0
    try:
        for token in tokenize.generate_tokens(iter(lines).__next__):
            tokens_by_line[-1].append(token)
            if (token.type == NEWLINE) \
                    or ((token.type == NL) and (parenlev <= 0)):
                tokens_by_line.append([])
            elif token.string in {'(', '[', '{'}:
                parenlev += 1
            elif token.string in {')', ']', '}'}:
                if parenlev > 0:
                    parenlev -= 1
    except tokenize.TokenError:
        # Input ended in a multiline string or expression. That's OK for us.
        pass


    if not tokens_by_line[-1]:
        tokens_by_line.pop()


    return tokens_by_line


def show_linewise_tokens(s: str):
    """For investigation and debugging"""
    if not s.endswith('\n'):
        s += '\n'
    lines = s.splitlines(keepends=True)
    for line in make_tokens_by_line(lines):
        print("Line -------")
        for tokinfo in line:
            print(" ", tokinfo)


def kql_code_score(lines:List[str])->bool:
    # remove indents
    lines = [line.lstrip() for line in lines]
    tok_lines = make_tokens_by_line(lines)

    comment_count = 0
    pipe_count = 0
    pipe_operators_count = 0
    table_operators_count = 0
    kql_count = 0
    negative_score = 0

    first_content_token = True
    last_token_type = None
    last_op = None
    for tok_line in tok_lines:
        for idx, tokinfo in enumerate(tok_line):

            if tokinfo.type in [tokenize.INDENT, tokenize.DEDENT]:
                continue

            if last_op == "//":
                if tokinfo.type in [tokenize.NEWLINE, tokenize.NL]:
                    last_op = None
                else:
                    continue

            elif tokinfo.type in [tokenize.COMMENT]:
                negative_score += 1

            elif tokinfo.type == tokenize.OP:
                if tokinfo.string == ">" and last_op == "|" and tokinfo.line[tokinfo.start[1] - 1] == "|":
                    kql_count += 1
                elif first_content_token or last_token_type in [tokenize.NEWLINE, tokenize.NL]:
                    if tokinfo.string in ["|", "//"]:
                        kql_count += 1
                    elif first_content_token and tokinfo.string in [".", "--"]:
                        kql_count += 1
                if tokinfo.string == "//":
                    comment_count += 1
                elif tokinfo.string == "|":
                    pipe_count += 1
                last_op = tokinfo.string
                first_content_token = False
            elif tokinfo.type in [tokenize.NEWLINE, tokenize.NL]:
                pass
            else:
                if last_op == "|":
                    scored = False
                    if tokinfo.type != tokenize.NAME:
                        scored = True
                        negative_score += 1

                    elif (idx + 2 < len(tok_line)
                          and tok_line[idx + 1].type == tokenize.OP
                          and tok_line[idx + 1].string == "-"
                          and tok_line[idx + 2].type == tokenize.NAME):

                        full_name = f"{tok_line[idx].string}{tok_line[idx + 1].string}{tok_line[idx + 2].string}"
                        if full_name in PIPE_OPERATORS2 \
                           and (tok_line[idx + 2].end[1] - tokinfo.start[1]) == len(full_name):

                            scored = True
                            pipe_operators_count += 1
                    
                    if not scored and tokinfo.string in PIPE_OPERATORS:
                        pipe_operators_count += 1
                
                elif first_content_token and tokinfo.string in TABLE_OPERATORS:
                    table_operators_count += 1

                first_content_token = False
                last_op = None

            last_token_type = tokinfo.type


PIPE_OPERATORS = [
    "as", "consume", "count", "distinct", "evaluate", "extend", "facet", "fork", 
    "getschema", "invoke", "join", "limit", "lookup", "order", "project", "parse",
    "partition", "reduce", "render", "sample", "search", "serialize", "sort",
    "summarize", "take", "top", "union", "where"

]

PIPE_OPERATORS2 = [
    "make-series", "mv-apply", "mv-expand", "project-away", "project-keep", "project-rename", "project-reorder",
    "parse-where", "sample-distinct", "top-nested", "top-hitters"
]

TABLE_OPERATORS = [
    "datatable", "externaldata", "find", "print", "range"
]
