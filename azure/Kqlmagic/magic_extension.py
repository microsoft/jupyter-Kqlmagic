# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from traitlets import TraitError

from .constants import Constants
from .log import logger
from .kql_magic import Kqlmagic
from .commands_table import COMMANDS_TABLE


def load_ipython_extension(ip):
    """Load the extension in Jupyter."""

    # this fails in both Firefox and Chrome for OS X.
    # I get the error: TypeError: IPython.CodeCell.config_defaults is undefined

    # js = "IPython.CodeCell.config_defaults.highlight_modes['magic_kql'] = {'reg':[/^%%kql/]};"
    # display_javascript(js, raw=True)
    logger().debug("load_ipython_extension - start")
    
    if not Kqlmagic.is_kqlmagic_kernel:
        result = _register_kqlmagic_magic(ip)
    else:
        result = None
        logger().debug(f"load_ipython_extension - cannot register {Constants.MAGIC_NAME} because magic is already by kqlmagic_kernel")

        
    logger().debug("load_ipython_extension - end")
    return result


def _register_kqlmagic_magic(ip, is_kqlmagic_kernel=False):
    """Register Kqlmagic magic in Jupyter."""

    if not Kqlmagic.is_kqlmagic_kernel:
        logger().debug(f"set_kqlmagic_magic - register magic {Constants.MAGIC_NAME}")
        # must be before registration
        Kqlmagic.is_ipython_extension = True
        Kqlmagic.is_kqlmagic_kernel = is_kqlmagic_kernel
        
        # register magic, also create magic object
        result = ip.register_magics(Kqlmagic)

        for alias in Constants.MAGIC_ALIASES:
            logger().debug(f"register_kqlmagic_magic - register '{alias}' cell alias for {Constants.MAGIC_NAME}")
            ip.magics_manager.register_alias(alias, Constants.MAGIC_NAME, "cell")

            logger().debug(f"register_kqlmagic_magic - register '{alias}' line alias for {Constants.MAGIC_NAME}")        
            ip.magics_manager.register_alias(alias, Constants.MAGIC_NAME, "line")

        if is_kqlmagic_kernel:
            _set_command_magics(ip)

    else:
        result = None

    return result


def _set_command_magics(ip):
    try:
        for cmd_name in COMMANDS_TABLE:
            flag:str = COMMANDS_TABLE.get(cmd_name).get("flag")
            is_cell:bool = COMMANDS_TABLE.get(cmd_name).get("cell")
            _register_command_magics(ip, flag, is_cell)
    except:
        pass


def _register_command_magics(ip, flag, is_cell):
    kind = "line"
    _register_command_kind_magics(ip, flag, kind)

    if is_cell:
        kind = "cell"
        _register_command_kind_magics(ip, flag, kind)


def _register_command_kind_magics(ip, flag, kind, params=None):
    params = params or f"--{flag}"

    _register_command_magic(ip, flag, params, kind)
    alias = flag.replace("_", "")
    if alias != flag:
        _register_command_magic(ip, alias, params, kind)
        alias = flag.replace("_", "-")
        _register_command_magic(ip, alias, params, kind)


def _register_command_magic(ip, alias:str, params, kind):
    try:
        logger().debug(f"_register_command_magic - register alias '{alias}' {kind} alias for {Constants.MAGIC_NAME} {params}")
        ip.magics_manager.register_alias(alias, Constants.MAGIC_NAME, magic_kind=kind, magic_params=params)

    except TraitError:
        logger().debug(f"_register_command_magic - failed to register alias '{alias}' {kind} alias for {Constants.MAGIC_NAME} {params}")
        if not alias.startswith("kql_"):
            alias = f"kql_{alias}"
            _register_command_kind_magics(ip, alias, kind, params)
        else:
            logger().debug(f"_register_command_magic - failed to register alias '{alias}' {kind} alias for {Constants.MAGIC_NAME} {params}")
    except:
        logger().debug(f"_register_command_magic - failed to register alias '{alias}' {kind} alias for {Constants.MAGIC_NAME} {params}")


def unload_ipython_extension(ip):
    """Unoad the extension in Jupyter."""
    logger().debug("unload_ipython_extension - start")
    
    if not Kqlmagic.is_kqlmagic_kernel:
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
    else:
        logger().debug(f"unload_ipython_extension - cannot remove {Constants.MAGIC_NAME} because magic is used by kqlmagic_kernel")
            
    logger().debug("unload_ipython_extension - end")
