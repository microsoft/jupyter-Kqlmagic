# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import sys
import platform
import subprocess

from typing import Any, Callable, Iterable


from ._debug_utils import debug_print
from traitlets.config.configurable import Configurable


class OsDependentAPI(object):
    """
    """

    options:Configurable = None

    def __init__(self, options:Configurable )->None:
        self.options = options
    
    
    @classmethod
    def webbrowser_open(cls, url:str)->None:
        # return webbrowser.open(url)
        # webbrowser.open(url, new=1, autoraise=True)
        if cls.options.get("notebook_app") in ["azuredatastudiosaw"]:
            try:
                OsDependentAPI.startfile(url)
                return
            except: # pylint: disable=bare-except
                pass
        try:
            import webbrowser
            webbrowser.open(url)
        except: # pylint: disable=bare-except
            OsDependentAPI.startfile(url)


    @classmethod
    def startfile(cls, filename:str)->None:
        platform = sys.platform
        if platform[:3] == "win":
            os.startfile(filename)
        else:
            opener = "open" if platform == "darwin" else "xdg-open"
            subprocess.call([opener, filename])