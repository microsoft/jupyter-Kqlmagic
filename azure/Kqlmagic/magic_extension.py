# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from .constants import Constants
from .log import logger
from .kql_magic import Kqlmagic


def load_ipython_extension(ip):
    """Load the extension in Jupyter."""

    # this fails in both Firefox and Chrome for OS X.
    # I get the error: TypeError: IPython.CodeCell.config_defaults is undefined

    # js = "IPython.CodeCell.config_defaults.highlight_modes['magic_kql'] = {'reg':[/^%%kql/]};"
    # display_javascript(js, raw=True)
    logger().debug("load_ipython_extension - start")

    logger().debug(f"load_ipython_extension - register {Constants.MAGIC_NAME}")
    # must be before registration
    Kqlmagic.is_ipython_extension = True
    # register magic, also create magic object
    result = ip.register_magics(Kqlmagic)

    for alias in Constants.MAGIC_ALIASES:
        logger().debug(f"load_ipython_extension - register '{alias}' cell alias for {Constants.MAGIC_NAME}")
        ip.magics_manager.register_alias(alias, Constants.MAGIC_NAME, "cell")

        logger().debug(f"load_ipython_extension - register '{alias}' line alias for {Constants.MAGIC_NAME}")        
        ip.magics_manager.register_alias(alias, Constants.MAGIC_NAME, "line")
        
    logger().debug("load_ipython_extension - end")
    return result


def unload_ipython_extension(ip):
    """Unoad the extension in Jupyter."""
    logger().debug("unload_ipython_extension - start")

    Kqlmagic.stop(unload_ipython_extension=True)
    Kqlmagic.is_ipython_extension = False
    logger().debug(f"unload_ipython_extension - remove {Constants.MAGIC_NAME} from cell magics")
    del ip.magics_manager.magics["cell"][Constants.MAGIC_NAME]

    logger().debug(f"unload_ipython_extension - remove {Constants.MAGIC_NAME} from line magics")
    del ip.magics_manager.magics["line"][Constants.MAGIC_NAME]


    for alias in Constants.MAGIC_ALIASES:
        logger().debug(f"unload_ipython_extension - remove '{alias}' cell alias for {Constants.MAGIC_NAME}")
        del ip.magics_manager.magics["cell"][alias]

        logger().debug(f"unload_ipython_extension - remove '{alias}' line alias for {Constants.MAGIC_NAME}")
        del ip.magics_manager.magics["line"][alias]

    logger().debug("unload_ipython_extension - end")
