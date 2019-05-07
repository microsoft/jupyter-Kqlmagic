# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from Kqlmagic.constants import Constants
from Kqlmagic.log import logger
from Kqlmagic.kql_magic import Kqlmagic as Magic


def load_ipython_extension(ip):
    """Load the extension in Jupyter."""

    # this fails in both Firefox and Chrome for OS X.
    # I get the error: TypeError: IPython.CodeCell.config_defaults is undefined

    # js = "IPython.CodeCell.config_defaults.highlight_modes['magic_kql'] = {'reg':[/^%%kql/]};"
    # display_javascript(js, raw=True)
    logger().debug("load_ipython_extension - start")

    logger().debug("load_ipython_extension - register {0}".format(Constants.MAGIC_NAME))
    result = ip.register_magics(Magic)

    for alias in Constants.MAGIC_ALIASES:
        logger().debug("load_ipython_extension - register '{0}' cell alias for {1}".format(alias, Constants.MAGIC_NAME))
        ip.magics_manager.register_alias(alias, Constants.MAGIC_NAME, "cell")

        logger().debug("load_ipython_extension - register '{0}' line alias for {1}".format(alias, Constants.MAGIC_NAME))        
        ip.magics_manager.register_alias(alias, Constants.MAGIC_NAME, "line")

        # ip.run_line_magic("alias_magic", "{0} {1}".format(alias, Constants.MAGIC_NAME))
        
    logger().debug("load_ipython_extension - end")
    return result


def unload_ipython_extension(ip):
    """Unoad the extension in Jupyter."""
    logger().debug("unload_ipython_extension - start")

    logger().debug("unload_ipython_extension - remove {0} from cell magics".format(Constants.MAGIC_NAME))
    del ip.magics_manager.magics["cell"][Constants.MAGIC_NAME]

    logger().debug("unload_ipython_extension - remove {0} from line magics".format(Constants.MAGIC_NAME))
    del ip.magics_manager.magics["line"][Constants.MAGIC_NAME]


    for alias in Constants.MAGIC_ALIASES:
        logger().debug("unload_ipython_extension - remove '{0}' cell alias for {1}".format(alias, Constants.MAGIC_NAME))
        del ip.magics_manager.magics["cell"][alias]

        logger().debug("unload_ipython_extension - remove '{0}' line alias for {1}".format(alias, Constants.MAGIC_NAME))
        del ip.magics_manager.magics["line"][alias]

    logger().debug("unload_ipython_extension - end")

