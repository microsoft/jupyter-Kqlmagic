# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from .magic_extension import load_ipython_extension, unload_ipython_extension
from .version import __version__
from .kql_magic import kql, kql_stop

__all__ = ['__version__', 'kql', 'kql_stop', 'load_ipython_extension', 'unload_ipython_extension']

