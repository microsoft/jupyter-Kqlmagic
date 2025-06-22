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
        from ctypes import Structure, POINTER, byref, create_string_buffer, windll, cdll
        from ctypes.wintypes import DWORD, PCHAR

        LocalFree = windll.kernel32.LocalFree
        memcpy = cdll.msvcrt.memcpy
        CryptProtectData = windll.crypt32.CryptProtectData
        CryptUnprotectData = windll.crypt32.CryptUnprotectData
    except Exception:
        dpapi_installed = False
    else:
        dpapi_installed = True
else:
    dpapi_installed = False


from .log import logger
from .constants import DpapiParam    


if dpapi_installed:

    CRYPTPROTECT_UI_FORBIDDEN = 0x01

    class DATA_BLOB(Structure):  # type: ignore  reportUnboundVariable

        _fields_ = [("cbData", DWORD), ("pbData", PCHAR)]  # type: ignore  reportUnboundVariable


    class DpapiCrypto(object):

        def __init__(self, **options):
            entropy: str = options.get(DpapiParam.ENTROPY) or "kqlmagic"
            self._entropy_blob_byref = byref(self._toBlob(entropy)) if entropy else None
            self._description: str = options.get(DpapiParam.DESCRIPTION)


        @property
        def suffix(self) -> str:
            return getpass.getuser()


        def _getData(self, blob)-> bytes:
            blob_length = int(blob.cbData)
            pbData = blob.pbData
            buffer = create_string_buffer(blob_length)
            memcpy(buffer, pbData, blob_length)
            LocalFree(pbData)
            return buffer.raw


        def _toBlob(self, data):
            data_bytes = str(data).encode() if isinstance(data, str) else data
            data_buffer = create_string_buffer(data_bytes)
            return DATA_BLOB(len(data_bytes), data_buffer)


        def encrypt(self, data: str)-> bytes:
            logger().debug(f"DpapiCrypto::encrypt(self, data)")
            if data:
                data_blob = self._toBlob(data)
                encrypted_blob = DATA_BLOB()
                if CryptProtectData(byref(data_blob), self._description, self._entropy_blob_byref, None, None,
                                    CRYPTPROTECT_UI_FORBIDDEN, byref(encrypted_blob)):
                    return self._getData(encrypted_blob)


        def decrypt(self, encrypted_data_bytes: bytes)-> str:
            logger().debug(f"DpapiCrypto::decrypt(self, encrypted_data_bytes)")
            if encrypted_data_bytes:
                encrypted_data_blob = self._toBlob(encrypted_data_bytes)
                data_blob = DATA_BLOB()
                if CryptUnprotectData(byref(encrypted_data_blob), None, self._entropy_blob_byref, None, None,
                                      CRYPTPROTECT_UI_FORBIDDEN, byref(data_blob)):
                    data_bytes = self._getData(data_blob)
                    return data_bytes.decode()


        def verify(self, encrypted_data_bytes: bytes) -> None:
            pass

else:
    class DpapiCrypto(object):

        def __init__(self, **options):
            pass
