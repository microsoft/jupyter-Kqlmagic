# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

try:
    # to avoid this warnig to be dispayed:
    # UserWarning: Distutils was imported before Setuptools. This usage is discouraged and may exhibit undesirable behaviors or errors. Please use Setuptools' objects directly or at least import Setuptools first.
    import setuptools
except:
    pass

from .magic_extension import load_ipython_extension, unload_ipython_extension, _register_kqlmagic_magic
from ._version import __version__
from .kql_magic import kql, kql_stop

__all__ = ['__version__', 'kql', 'kql_stop', 'load_ipython_extension', 'unload_ipython_extension', '_register_kqlmagic_magic']
