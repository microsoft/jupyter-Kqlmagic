# DPAPI access library
# This file uses code originally created by Crusher Joe:
# http://article.gmane.org/gmane.comp.python.ctypes/420
#


from ctypes import Structure, POINTER, byref, create_string_buffer, windll, cdll
from ctypes.wintypes import DWORD, PCHAR

from .constants import DpapiParam

LocalFree = windll.kernel32.LocalFree
memcpy = cdll.msvcrt.memcpy

CryptProtectData = windll.crypt32.CryptProtectData
CryptUnprotectData = windll.crypt32.CryptUnprotectData
CRYPTPROTECT_UI_FORBIDDEN = 0x01


class DATA_BLOB(Structure):
    _fields_ = [("cbData", DWORD), ("pbData", PCHAR)]

class DPAPI(object):

    def __init__(self, **options):
        _salt: str = options.get(DpapiParam.SALT)
        self.salt_blob_byref = byref(self._toBlob(_salt)) if _salt else None
        self.description: str = options.get(DpapiParam.DESCRIPTION)


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
        if data:
            data_blob = self._toBlob(data)
            encrypted_blob = DATA_BLOB()
            if CryptProtectData(byref(data_blob), self.description, self.salt_blob_byref, None, None,
                                    CRYPTPROTECT_UI_FORBIDDEN, byref(encrypted_blob)):
                return self._getData(encrypted_blob)


    def decrypt(self, encrypted_data_bytes: bytes)-> str:
        if encrypted_data_bytes:
            encrypted_data_blob = self._toBlob(encrypted_data_bytes)
            data_blob = DATA_BLOB()
            if CryptUnprotectData(byref(encrypted_data_blob), None, self.salt_blob_byref, None, None,
                                    CRYPTPROTECT_UI_FORBIDDEN, byref(data_blob)):
                data_bytes = self._getData(data_blob)
                return data_bytes.decode()
