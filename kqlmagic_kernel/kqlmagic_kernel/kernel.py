# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import json
import os
import sys
from traitlets import Dict, Unicode
from ipykernel.ipkernel import Kernel
from ._version import __version__ as kqlmagic_version


from ._version import __version__


HELP_LINKS = [
    {
        'text': "Kqlmagic Kernel",
        'url': "https://github.com/Microsoft/jupyter-Kqlmagic-Kernel",
    },
    {
        'text': "Kqlmagic",
        'url': "https://github.com/Microsoft/jupyter-Kqlmagic",
    },
    {
        'text': "Kql cheat sheet",
        'url': "https://github.com/marcusbakker/KQL", 
    }

] # + Kernel.help_link

def get_kernel_json():
    """Get the kernel json for the kernel.
    """

    here = os.path.dirname(__file__)
    default_json_file = os.path.join(here, "spec", 'kernel.json')
    json_file = os.environ.get('KQLMAGIC_KERNEL_JSON', default_json_file)
    with open(json_file) as fid:
        data = json.load(fid)
    data['argv'][0] = sys.executable
    return data

class KqlmagicKernel(Kernel):
    app_name = 'kqlmagic_kernel'
    implementation = 'Kqlmagic Kernel'
    implementation_version = __version__
    language = 'kql'
    help_links = HELP_LINKS
    kernel_json = Dict(get_kernel_json()).tag(config=True)
    cli_options = Unicode('').tag(config=True)
    inline_toolkit = Unicode('').tag(config=True)

    _language_version = "1"

    @property
    def language_version(self):
        if self._language_version:
            return self._language_version
        return "1.0"

    @property
    def language_info(self):
        info = Kernel.language_info.copy()
        info['name'] = 'kql'
        info['version'] = self.language_version
        info['help_links'] = HELP_LINKS
        return info

    @property
    def banner(self):
        msg = 'Kqlmagic Kernel v%s running Kqlmagic v%s'
        return msg % (__version__, kqlmagic_version)


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._start_kqlmagic()

    def _start_kqlmagic(self):
        env = os.environ
        env["KQLMAGIC_KERNEL"] = "True"
        import Kqlmagic
        from IPython import get_ipython
        # Kqlmagic.load_ipython_extension(get_ipython())
        Kqlmagic._register_kqlmagic_magic(get_ipython(), is_kqlmagic_kernel=True)
        # disable unload_ipython_extension()
        # disable activate/deactivate kernel