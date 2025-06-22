# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""A module to execute, in %%kql cell, python code using user's namespace
"""

from typing import Any, Dict
import ast
import copy


from .constants import Constants
from .ipython_api import IPythonAPI


class CompileMode(object):
    EXEC        =   "exec"
    EVAL        =   "eval"
    SINGLE      = "single"
    INTERACTIVE = "single"


NO_FILENAME:str = "<string>"


def execute_python_command(py_code:str, user_ns:Dict[str,Any], **options)->Any:

    tr_py_code = IPythonAPI.transform_cell(Constants.DONT_ADD_CELL_MAGIC_PREFIX + py_code)
    return _exec_with_return(tr_py_code, user_ns)


def _convertExprStmt2Expression(expr_stmt:ast.Expr)->Any:
    expr_stmt = copy.deepcopy(expr_stmt)
    expr_stmt.lineno = 0
    expr_stmt.col_offset = 0

    expr_stmt_value = expr_stmt.value

    stmt_expression = ast.Expression(expr_stmt_value, lineno=0, col_offset=0)
    return stmt_expression


def _exec_with_return(py_code:str, user_ns:Dict[str,Any])->Any:
    """if last statement is an expression, split execution to two parts:
    execute all statements except last
    evaluate last statement and return result
    """

    if py_code is None:
        return None

    all_stmts_ast = ast.parse(py_code)

    if len(all_stmts_ast.body) > 0 and type(all_stmts_ast.body[-1]) == ast.Expr:

        last_stmt_expression = _convertExprStmt2Expression(all_stmts_ast.body[-1])

        all_stmts_except_last_ast = all_stmts_ast
        all_stmts_except_last_ast.body = all_stmts_except_last_ast.body[:-1]

        exec(compile(all_stmts_except_last_ast, NO_FILENAME, CompileMode.EXEC), user_ns)
        return eval(compile(last_stmt_expression, NO_FILENAME, CompileMode.EVAL), user_ns)
    else:
        exec(compile(all_stmts_ast, NO_FILENAME, CompileMode.EXEC), user_ns)
