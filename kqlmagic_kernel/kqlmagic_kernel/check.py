
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import sys
import os
import platform
import traceback
from IPython.core import release
from . import __version__
from Kqlmagic._version import __version__ as _kqlmagic_version
from Kqlmagic.constants import Constants
from .kernel import KqlmagicKernel


if __name__ == "__main__":
    try:
        print(f'Python version: v{sys.version}')
        python_branch = platform.python_branch()
        if python_branch is not None and len(python_branch) > 0 and not python_branch.isspace(): 
            print(f'python branch: {platform.python_branch()}')
        print(f'Python path: {sys.executable}')

        print(f'\nIPython.core version: v{release.__version__}')

        print(f'\nKqlmagic kernel version: v{__version__}')
        print(f'Kqlmagic module version: v{_kqlmagic_version}')

        print('\nConnecting to Kqlmagic...')

        os.environ[f"{Constants.MAGIC_CLASS_NAME_UPPER}_LOAD_MODE"] = "silent"
        o = KqlmagicKernel()
        print('...Kqlmagic connection established')
        print(f"Kqlmagic banner: {o.banner}")

        result_var = "_"
        cell1 = f"--version" # \n\n%py {result_var}"# {result_var}=_kql_last_result_\n%py {result_var}"
        cell1_str = cell1.replace("\n", "\\n")
        user_expressions = {"result_object": f"{result_var}"}
        ver = o.do_execute(code=cell1, silent=True, store_history=False, user_expressions=user_expressions, allow_stdin=False)
        # print(f"\nKqlmagic execute: \"{cell1_str}\", {result_var}: \"{o.shell.user_ns.get(result_var, 'None')}\", return: {ver.result()}")
        print(f"\nKqlmagic execute: \"{cell1_str}\", {result_var}: \"{o.shell.user_ns.get(result_var, 'None')}\"")
        print(f"Kqlmagic execute: \"{cell1_str}\", \"text/plain\": \"{ver.result()['user_expressions']['result_object']['data']['text/plain']}\"")

        # result_var = "ver"
        # cell2 = f"%py {result_var} = %kql --version"
        # user_expressions = {"result_object": f"{result_var}"}
        # ver = o.do_execute(code=cell2, silent=True, store_history=False, user_expressions=user_expressions, allow_stdin=False)
        # print(f"\nKqlmagic execute: \"{cell2}\", {result_var}: {o.shell.user_ns.get(result_var, 'None')}, return: {ver.result()}")

    except Exception as e:
        print(traceback.format_exc())