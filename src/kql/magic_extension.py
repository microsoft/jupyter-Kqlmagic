#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

from kql.constants import Constants
from kql.kql_magic import Kqlmagic as Magic

def load_ipython_extension(ip):
    """Load the extension in Jupyter."""

    # this fails in both Firefox and Chrome for OS X.
    # I get the error: TypeError: IPython.CodeCell.config_defaults is undefined

    # js = "IPython.CodeCell.config_defaults.highlight_modes['magic_kql'] = {'reg':[/^%%kql/]};"
    # display_javascript(js, raw=True)
    result = ip.register_magics(Magic)
    for alias in Constants.MAGIC_ALIASES:
        ip.magics_manager.register_alias(alias, Constants.MAGIC_NAME, 'cell')
        ip.magics_manager.register_alias(alias, Constants.MAGIC_NAME, 'line')
        # ip.run_line_magic("alias_magic", "{0} {1}".format(alias, Constants.MAGIC_NAME))
    return result

def unload_ipython_extension(ip):
    """Unoad the extension in Jupyter."""
    del ip.magics_manager.magics["cell"][Constants.MAGIC_NAME]
    del ip.magics_manager.magics["line"][Constants.MAGIC_NAME]
    for alias in Constants.MAGIC_ALIASES:
        del ip.magics_manager.magics["cell"][alias]
        del ip.magics_manager.magics["line"][alias]
