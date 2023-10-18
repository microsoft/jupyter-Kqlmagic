# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, Dict
import os
import uuid
import base64


from .dependencies import Dependencies
from .constants import CryptoParam
from .log import logger


class FernetCrypto(object):

    _is_installed = None
    _are_dependencies_set = False
    fernet = None
    default_backend = None
    hashes = None
    PBKDF2HMAC = None
    password_policy = None
    password_tests = None

    @classmethod
    def set_dependencies(cls)->str:
        if not cls._are_dependencies_set:
            fernet_module = Dependencies.get_module("cryptography.fernet", dont_throw=True)
            hazmat_backends = Dependencies.get_module("cryptography.hazmat.backends", dont_throw=True)
            hazmat_primitives = Dependencies.get_module("cryptography.hazmat.primitives", dont_throw=True)
            hazmat_primitives_kdf_pbkdf2 = Dependencies.get_module("cryptography.hazmat.primitives.kdf.pbkdf2", dont_throw=True)
            password_strength = Dependencies.get_module("password_strength", dont_throw=True)
            cls._are_dependencies_set = True

            cls._is_installed = (fernet_module 
                                and hazmat_backends 
                                and hazmat_primitives 
                                and hazmat_primitives_kdf_pbkdf2
                                and password_strength)

            if cls._is_installed:
                cls.fernet = fernet_module.Fernet
                cls.default_backend = hazmat_backends.default_backend
                cls.hashes = hazmat_primitives.hashes
                cls.PBKDF2HMAC = hazmat_primitives_kdf_pbkdf2.PBKDF2HMAC
                cls.password_policy = password_strength.PasswordPolicy
                cls.password_tests = password_strength.tests


    @classmethod
    def is_installed(cls)->str:
        if cls._is_installed is None:
            cls.set_dependencies()
        return cls._is_installed


    def __init__(self, options:Dict[str,Any])->None:
        self.set_dependencies()
        if self.is_installed():
            self._crypto_key = options.get(CryptoParam.CRYPTO_KEY) or self._create_encryption_key(options)
            self._crypto = self._init_crypto(self._crypto_key)
            self._suffix = self._create_suffix_from_key(self._crypto_key)


    @property
    def suffix(self)->str:
        return self._suffix


    @property
    def crypto_key(self)->str:
        return self._crypto_key


    def _create_suffix_from_key(self, key:bytes)->str:
        last_two_bytes = [b for b in key[-2:]]
        _suffix = last_two_bytes[0] * 256 + last_two_bytes[1]
        return _suffix


    def _create_encryption_key(self, options:Dict[str,Any])->bytes:
        password = options.get(CryptoParam.PASSWORD, str(uuid.uuid4()))
        password_as_bytes = password.encode()
        salt  = options.get(CryptoParam.SALT, uuid.uuid4())
        logger().debug(f"_create_encryption_key {type(salt)}")
        salt_bytes = str(salt).encode()
        length = options.get(CryptoParam.LENGTH, 32)
        length = min(length, 32) + 2  # 2 last bytes are to create suffix
        iterations = options.get(CryptoParam.ITERATIONS, 100000)
        algorithm = options.get(CryptoParam.ALGORITHM, self.hashes.SHA256())
        backend = options.get(CryptoParam.BACKEND, self.default_backend()) # pylint: disable=not-callable

        kdf = self.PBKDF2HMAC(algorithm=algorithm, length=length, salt=salt_bytes, iterations=iterations, backend=backend) # pylint: disable=not-callable
        key = kdf.derive(password_as_bytes)

        # PBKDF2 instances can only be used once
        kdf = self.PBKDF2HMAC(algorithm=algorithm, length=length, salt=salt_bytes, iterations=iterations, backend=backend) # pylint: disable=not-callable
        kdf.verify(password_as_bytes, key)
        return key


    def _init_crypto(self, key:bytes):
        if key:
            key_len = len(key)
            # make the key long enough or short enough 
            diff = 32 - key_len

            _key = key
            if diff < 0:
                _key = key[:diff]
            elif diff > 0:
                # logger().debug(f"_init_crypto {type(_key)}")
                _key = key +  ("0" * diff).encode()
                # logger().debug(f"_init_crypto {type(_key)}")

            _key = base64.urlsafe_b64encode(_key)
        else:
            _key = self.fernet.generate_key()
        return self.fernet(_key) # pylint: disable=not-callable


    def encrypt(self, data:str)->bytes:
        logger().debug(f"encrypt {type(data)}")

        if data:
            return self._crypto.encrypt(data.encode()) 


    def decrypt(self, encrypted_data_bytes:bytes)->str:
        if encrypted_data_bytes:
            data_as_bytes = self._crypto.decrypt(encrypted_data_bytes)
            return data_as_bytes.decode()


    def verify(self, encrypted_data_bytes:bytes)->None:
        "raise exception if metadata is invalid"
        if encrypted_data_bytes:
            self.fernet._get_unverified_token_data(encrypted_data_bytes)


    def _timestamp(self, encrypted_data_bytes:bytes)->int:
        if encrypted_data_bytes:
            timestamp, data = self.fernet._get_unverified_token_data(encrypted_data_bytes)  # pylint: disable=unused-variable
            return timestamp
        else:
            return 0


    @classmethod
    def check_password_strength(cls, password:str)->str:
        password_hints = {
            cls.password_tests.Length: "at least 8 characters.",
            cls.password_tests.Uppercase: "at least one uppercase letter.",
            cls.password_tests.Numbers: "at least 2 digits.",
            cls.password_tests.NonLetters: "at least one non-letter character"
        }

        policy = cls.password_policy.from_names(
            length=8,  # min length: 8
            uppercase=1,  # need min. 1 uppercase letters
            numbers=2,  # need min. 2 digits
            nonletters=1,  # need min. 2 non-letter characters (digits, specials, anything) 
        )

        results = policy.test(password)

        if len(results) > 0:
            hints = []
            for i, policy in enumerate(results,1):
                hints.append(f"{i}. {password_hints.get(type(policy))}")
                return os.linesep.join(hints)
