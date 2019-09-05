# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import uuid
import base64


try:
    from cryptography.fernet import Fernet
    from cryptography.fernet import InvalidToken

    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

    from password_strength import PasswordPolicy
    from password_strength import tests
except Exception:
    fernet_installed = False
else:
    fernet_installed = True


from .constants import CryptoParam
from .log import logger


if fernet_installed:

    class FernetCrypto(object):

        def __init__(self, options: dict):
            key = self._create_encryption_key(options)
            self._crypto = self._init_crypto(key)
            self._suffix = self._create_suffix_from_key(key)


        @property
        def suffix(self) -> str:
            return self._suffix


        def _create_suffix_from_key(self, key: bytes)-> str:
            last_two_bytes = [b for b in key[-2:]]
            _suffix = last_two_bytes[0]*256 + last_two_bytes[1]
            return _suffix


        def _create_encryption_key(self, options: dict) -> bytes:
            password = options.get(CryptoParam.PASSWORD, str(uuid.uuid4()))
            password_as_bytes = password.encode()
            salt  = options.get(CryptoParam.SALT, uuid.uuid4())
            logger().debug(f"_create_encryption_key {type(salt)}")
            salt_bytes = str(salt).encode()
            length = options.get(CryptoParam.LENGTH, 32)
            length = min(length, 32) + 2 # 2 last bytes are to create suffix
            iterations = options.get(CryptoParam.ITERATIONS, 100000)
            algorithm = options.get(CryptoParam.ALGORITHM, hashes.SHA256())
            backend = options.get(CryptoParam.BACKEND, default_backend())

            kdf = PBKDF2HMAC(algorithm=algorithm, length=length, salt=salt_bytes, iterations=iterations, backend=backend)
            key = kdf.derive(password_as_bytes)

            kdf = PBKDF2HMAC(algorithm=algorithm, length=length, salt=salt_bytes, iterations=iterations, backend=backend) #PBKDF2 instances can only be used once
            kdf.verify(password_as_bytes, key)
            return key


        def _init_crypto(self, key: bytes) -> Fernet:
            if key:
                key_len = len(key)
                #make the key long enough or short enough 
                diff = 32 - key_len

                if diff < 0:
                    _key = key[:diff]
                elif diff > 0:
                    # logger().debug(f"_init_crypto {type(_key)}")
                    _key = key +  ("0"*diff).encode()
                    # logger().debug(f"_init_crypto {type(_key)}")

                _key = base64.urlsafe_b64encode(_key)
            else:
                _key = Fernet.generate_key()
            return Fernet(_key)


        def encrypt(self, data: str) -> bytes:
            logger().debug(f"encrypt {type(data)}")

            if data:
                return self._crypto.encrypt(data.encode()) 


        def decrypt(self, encrypted_data_bytes: bytes) -> str:
            if encrypted_data_bytes:
                data_as_bytes = self._crypto.decrypt(encrypted_data_bytes)
                return data_as_bytes.decode()


        def verify(self, encrypted_data_bytes: bytes) -> None:
            "raise exception if metadata is invalid"
            if encrypted_data_bytes:
                Fernet._get_unverified_token_data(encrypted_data_bytes)


        def _timestamp(self, encrypted_data_bytes: bytes)-> int:
            if encrypted_data_bytes:
                timestamp, data = Fernet._get_unverified_token_data(encrypted_data_bytes) # pylint: disable=unused-variable
                return timestamp
            else:
                return 0


    def check_password_strength(password) -> str:
        password_hints = {
            tests.Length: "at least 8 characters.",
            tests.Uppercase: "at least one uppercase letter.",
            tests.Numbers: "at least 2 digits.",
            tests.NonLetters: "at least one non-letter character"
        }

        policy = PasswordPolicy.from_names(
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

else:
    class FernetCrypto(object):

        def __init__(self, options: dict):
            pass

    def check_password_strength(password) -> str:
        pass