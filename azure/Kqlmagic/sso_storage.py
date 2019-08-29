import os
import time
import uuid

from .parser import Parser
from .display import Display
from .constants import Constants, CryptoParam, SsoStorageParam, SsoEnvVarParam
from .crypto import Crypto, cryptography_installed
from .log import logger
from .dpapi import DPAPI
from datetime import datetime, timedelta

class SsoStorage(object):
    last_clear_time = datetime.utcnow()

    def __init__(self, sso_options: dict, state=None):
        self.authority = sso_options.get("authority")
        self.db = sso_options.get(SsoStorageParam.DB, {})
        self.gc_ttl_in_secs = sso_options.get(SsoStorageParam.GC_TTL_IN_SECS, 0) 
        self._crypto = sso_options.get(SsoStorageParam.CRYPTO)
        self.db_key = self._get_db_key(sso_options.get(SsoStorageParam.CACHE_NAME, "sso"), self._crypto.suffix, self.authority)

        self.db_key_conflict = False
        
        self.restore() #to throw warnings\errors 
        if state:
            self.save(state)


    def _get_db_key(self, cache_name: str, suffix: bytes, authority:str) -> str:
        
        sso_db_key_authority = authority[authority.find("://")+3:] if authority else ""
        if authority:
            path = os.path.join(Constants.SSO_DB_KEY_PREFIX, cache_name + str(suffix), sso_db_key_authority)
        else:
            path = os.path.join(Constants.SSO_DB_KEY_PREFIX, cache_name + str(suffix))
        return path


    def db_gc(self):
        '''db garbage collector. remove old entries'''
        logger().debug(f"SsoStorage(object)::db_gc ")
        for db_key, db_value in self.db.items():
            logger().debug(f"SsoStorage(object)::db_gc db_key, db_value in self.db.items() {db_key}  , {db_value}  in self.db.items() ")

            if db_key.startswith(Constants.SSO_DB_KEY_PREFIX):
                state_encrypted = db_value
                if not state_encrypted:
                    continue
                time_created = state_encrypted.get("timestamp")
                logger().debug(f"SsoStorage(object)::time_created {time_created}  timedelta(seconds=self.gc_ttl_in_secs) {timedelta(seconds=self.gc_ttl_in_secs)}")

                if (time_created + timedelta(seconds=self.gc_ttl_in_secs)) < datetime.utcnow():
                    del self.db[db_key]



    def clear_sso_db(self):
        '''clear db. remove all entries'''
        for db_key in self.db.keys():
            logger().debug(f"SsoStorage(object):: clear_sso_db db_key,{db_key} ")
            logger().debug(f"SsoStorage(object):: clear_sso_db self db_key,{self.db_key} ")

            if db_key.startswith(self.db_key):
                logger().debug(f"SsoStorage(object):: in startswith clear_sso_db db_key,{db_key} ")
                del self.db[db_key]


    def save(self, state: str) -> None:
        '''save cache state in db'''
        logger().debug(f"SsoStorage(object)::save(self, state: str) -> None state is {type(state)}")

        if not self.db_key_conflict:
            state_encrypted = self._crypto.encrypt(state)  
            self.db[self.db_key] = {'data': state_encrypted, 'timestamp': datetime.utcnow()}


    def restore(self) -> str:
        '''restore cache state from db'''
        if self.gc_ttl_in_secs>0 and (self.last_clear_time + timedelta(seconds=self.gc_ttl_in_secs)) < datetime.utcnow():
            self.last_clear_time = datetime.utcnow()
            self.db_gc()

        if self.db_key_conflict:
            return
        
        state_encrypted = self.db.get(self.db_key)
        if not state_encrypted:
            return

        try:
            return self._crypto.decrypt(state_encrypted.get("data"))

        except:
            try: 
                self.db_key_conflict = True

                self._crypto.verify(state_encrypted) 
                Display.showWarningMessage("Warning: SSO disabled, due to cache_name conflict")

            except: #the token has bad form
                Display.showWarningMessage("Warning: SSO disabled, due to cache_name conflict (invalid data in db)")
                # del self.db[self.db_key]


def get_sso_store(authority = None, **options) -> SsoStorage: #pylint: disable=no-method-argument
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
    crypto = DPAPI()

    gc_ttl_in_secs = options.get('sso_cleanup_interval', 0) * Constants.HOUR_SECS #convert from hours to seconds
    ip = get_ipython()  # pylint: disable=undefined-variable

    sso_storage_options = {
        "authority": authority,
        SsoStorageParam.DB: ip.db,
        SsoStorageParam.CRYPTO: crypto,
        SsoStorageParam.CACHE_NAME: cache_name,
        SsoStorageParam.GC_TTL_IN_SECS: gc_ttl_in_secs
    }
    return SsoStorage(sso_storage_options)
