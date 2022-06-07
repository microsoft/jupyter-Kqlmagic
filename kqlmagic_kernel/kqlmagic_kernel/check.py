
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import sys
from IPython.core import release
from . import __version__
from Kqlmagic._version import __version__ as _kqlmagic_version
from .kernel import KqlmagicKernel


if __name__ == "__main__":
    print('Kqlmagic kernel v%s' % __version__)
    print('Kqlmagic v%s' % _kqlmagic_version)
    print('IPython.core v%s' % release.__version__)
    print('Python v%s' % sys.version)
    print('Python path: %s' % sys.executable)
    print('\nConnecting to Kqlmagic...')
    try:
        o = KqlmagicKernel()
        print('Kqlmagic connection established')
        print(o.banner)
        ver = o.do_execute(code="--version", silent=True, store_history=False)
        print('Kqlmagic --version: %s' % ver)
    except Exception as e:
        print(e)