import os
import uuid
import base64
from .constants import CryptoParam

try:
    from cryptography.fernet import Fernet
    from cryptography.fernet import InvalidToken

    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

    from password_strength import PasswordPolicy
    from password_strength import tests
except Exception:
    cryptography_installed = False
else:
    cryptography_installed = True


class Crypto(object):

    def __init__(self, options: dict):
        self.key = self._create_encryption_key(options)
        self._cypto = self._init_crypto(self.key)


    def _create_encryption_key(self, options: dict) -> bytes:
        password = options.get(CryptoParam.PASSWORD, uuid.uuid4())
        password_as_bytes = password.encode()
        salt  = options.get(CryptoParam.SALT, uuid.uuid4())
        salt_bytes = str(salt).encode()
        length = options.get(CryptoParam.LENGTH, 32)
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
            _key = key if len(key) == 32 else [(key[idx] if idx<key_len else 0) for idx in range(32)]
            _key = base64.urlsafe_b64encode(_key)
        else:
            _key = Fernet.generate_key()
        return Fernet(_key)


    def encrypt(self, data: str) -> bytes:
        if data:
            data_as_bytes = data.encode()
            return self._cypto.encrypt(data_as_bytes) 


    def decrypt(self, encrypted_data_as_bytes: bytes) -> str:
        if encrypted_data_as_bytes:
            data_as_bytes = self._cypto.decrypt(encrypted_data_as_bytes)
            return data_as_bytes.decode()


    def verify(self, encrypted_data_as_bytes: bytes) -> None:
        "raise exception if metadata is invalid"
        if encrypted_data_as_bytes:
            Fernet._get_unverified_token_data(encrypted_data_as_bytes)


    def timestamp(self, encrypted_data_as_bytes: bytes)-> int:
        if encrypted_data_as_bytes:
            timestamp, data = Fernet._get_unverified_token_data(encrypted_data_as_bytes) # pylint: disable=unused-variable
            return timestamp
        else:
            return 0

    @staticmethod
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

