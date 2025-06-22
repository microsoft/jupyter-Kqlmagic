# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

# DPAPI access library
# This file uses code originally created by Crusher Joe:
# http://article.gmane.org/gmane.comp.python.ctypes/420
#

import platform
import getpass


if platform.system() == 'Windows':
    try:
        from msal_extensions.windows import WindowsDataProtectionAgent
    except Exception:
        dpapi_installed = False
    else:
        dpapi_installed = True
else:
    dpapi_installed = False


from .log import logger
from .constants import DpapiParam, SsoCrypto    


if dpapi_installed:

    class DpapiCrypto(object):

        def __init__(self, **crypto_options)->None:
            entropy: str = crypto_options.get(DpapiParam.ENTROPY) or "kqlmagic"
            self._description: str = crypto_options.get(DpapiParam.DESCRIPTION)
            self._dp_agent = WindowsDataProtectionAgent(entropy=entropy)


        @property
        def suffix(self)->str:
            return getpass.getuser()


        def encrypt(self, data:str)->bytes:
            logger().debug(f"DpapiCrypto(object)::encrypt(self, data)")
            if data:
                return self._dp_agent.protect(data)


        def decrypt(self, encrypted_data_bytes:bytes)->str:
            logger().debug(f"DpapiCrypto(object)::decrypt(self, encrypted_data_bytes)")
            if encrypted_data_bytes:
                return self._dp_agent.unprotect(encrypted_data_bytes)


        def verify(self, encrypted_data_bytes:bytes)->None:
            pass

else:
    class DpapiCrypto(object):

        def __init__(self, **crypto_options):
            raise Exception(f"Warning: SSO is not activated due to {SsoCrypto.DPAPI} cryptography failed to import WindowsDataProtectionAgent from msal_extensions.windows")
