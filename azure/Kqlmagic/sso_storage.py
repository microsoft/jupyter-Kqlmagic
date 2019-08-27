import os
import time
import uuid

from .parser import Parser
from .display import Display
from .constants import Constants, CryptoParam, SsoStorageParam, SsoEnvVarParam
from .crypto import Crypto, cryptography_installed


class SsoStorage(object):
    last_clear_time = int(time.time())

    def __init__(self, sso_options: dict, state=None):
        self.db = sso_options.get(SsoStorageParam.DB, {})
        self.gc_ttl_in_secs = sso_options.get(SsoStorageParam.GC_TTL_IN_SECS, 0) 
        self._crypto = sso_options.get(SsoStorageParam.CRYPTO)
        self.db_key = self._get_db_key(sso_options.get(SsoStorageParam.CACHE_NAME, "sso"), self._crypto.key)

        self.db_key_conflict = False
        self.restore()
        if state:
            self.save(state)


    def _get_db_key(self, cache_name: str, key: bytes) -> str:
        last_two_bytes = [b for b in key[-2:]]
        suffix = last_two_bytes[0]*256 + last_two_bytes[1]
        return f"{Constants.SSO_DB_KEY_PREFIX}{cache_name + str(suffix)}"


    def db_gc(self):
        '''db garbage collector. remove old entries'''
        for db_key, db_value in self.db.items():
            if db_key.startswith(Constants.SSO_DB_KEY_PREFIX):
                state_encrypted = db_value
                if not state_encrypted:
                    continue
                try:
                    time_created = self._crypto.timestamp(state_encrypted)
                    if (time_created + self.gc_ttl_in_secs) < int(time.time()):
                        del self.db[db_key]
                except:
                    pass


    def save(self, state: str) -> None:
        '''save cache state in db'''
        if not self.db_key_conflict:
            state_as_bytes = state.encode()
            state_encrypted = self._crypto.encrypt(state_as_bytes)  
            self.db[self.db_key] = state_encrypted


    def restore(self) -> str:
        '''restore cache state from db'''
        if (self.gc_ttl_in_secs > 0 and (self.last_clear_time + Constants.SSO_GC_INTERVAL_IN_SECS) < int(time.time()) ):
            self.last_clear_time = int(time.time())
            self.db_gc()

        if self.db_key_conflict:
            return
        
        state_encrypted = self.db.get(self.db_key)
        if not state_encrypted:
            return

        try:
            return self._crypto.decrypt(state_encrypted)

        except:
            try: 
                self.db_key_conflict = True

                self._crypto.verify(state_encrypted)
                Display.showWarningMessage("Warning: SSO disabled, due to cache_name conflict")

            except: #the token has bad form
                Display.showWarningMessage("Warning: SSO disabled, due to cache_name conflict (invalid data in db)")
                # del self.db[self.db_key]


def get_sso_store(**options) -> SsoStorage: #pylint: disable=no-method-argument
    encryption_keys_string = os.getenv(Constants.SSO_ENV_VAR_NAME)
    if not encryption_keys_string:
        # Display.showWarningMessage(f"Warning: SSO is not activated because environment variable {SSO_ENV_VAR_NAME} is not set")
        return

    if not cryptography_installed:
        Display.showWarningMessage("Warning: SSO is not activated due to cryptography and/or password-strength modules are not found")
        return

    key_vals = Parser.parse_and_get_kv_string(encryption_keys_string, {})
    cache_name = key_vals.get(SsoEnvVarParam.CACHE_NAME)  
    secret_key = key_vals.get(SsoEnvVarParam.SECRET_KEY)
    secret_salt_uuid = key_vals.get(SsoEnvVarParam.SECRET_SALT_UUID)

    if not(secret_salt_uuid and secret_key and cache_name):
        Display.showWarningMessage(f"Warning: SSO is not activated due to environment variable {Constants.SSO_ENV_VAR_NAME} is missing some keys")
        return

    hint = Crypto.check_password_strength(secret_key)
    if hint:
        message = f"Warning: SSO could not be activated due to secret_key key in environment variable {Constants.SSO_ENV_VAR_NAME} is too simple. It should contain: {os.linesep}{hint}" 
        Display.showWarningMessage(message)
        return

    try:
        salt = uuid.UUID(secret_salt_uuid, version=4)
    except:
        Display.showWarningMessage(f"Warning: SSO is not activated due to secret_salt_uuid key in environment variable {Constants.SSO_ENV_VAR_NAME} is not set to a valid uuid")
        return

    crypto_options = {
        CryptoParam.PASSWORD: cache_name + secret_key,
        CryptoParam.SALT: salt,
        CryptoParam.LENGTH: 34
    }
    crypto = Crypto(crypto_options)

    gc_ttl_in_secs = options.get('sso_cleanup_interval', 0) * Constants.HOUR_SECS #convert from hours to seconds
    ip = get_ipython()  # pylint: disable=undefined-variable

    sso_storage_options = {
        SsoStorageParam.DB: ip.db,
        SsoStorageParam.CRYPTO: crypto,
        SsoStorageParam.CACHE_NAME: cache_name,
        SsoStorageParam.GC_TTL_IN_SECS: gc_ttl_in_secs
    }
    return SsoStorage(sso_storage_options)
