# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import platform
import uuid



from .parser import Parser
from .display import Display
from .ipython_api import IPythonAPI
from .constants import Constants, CryptoParam, SsoStorageParam, SsoEnvVarParam, SsoStorage, SsoCrypto
from .fernet_crypto import FernetCrypto
from .dpapi_crypto_msal import DpapiCrypto

from .dict_db_storage import DictDbStorage


_SUPPORTED_SSO_STORAGE_TYPES = [
    SsoStorage.IPYTHON_DB
]


_SUPPORTED_SSO_CRYPTO_TYPES = [
    SsoCrypto.DPAPI,
    SsoCrypto.LINUX_LIBSECRET,
    SsoCrypto.OSX_KEYCHAIN,
    SsoCrypto.FERNET
]


class SsoWarning(Exception):
    """Single Sign On Warning class."""

    pass


sso_storage = None


def get_sso_store(cache_selector_key:str=None, **options) -> DictDbStorage:
    global sso_storage
    encryption_keys_string = os.getenv(Constants.SSO_KEYS_ENV_VAR_NAME)
    key_vals = Parser.parse_and_get_kv_string(encryption_keys_string, {}) if encryption_keys_string is not None else {}
    # Display.showWarningMessage(f"Warning: SSO is not activated because environment variable {SSO_KEYS_ENV_VAR_NAME} is not set")

    try:
        crypto_obj = None

        crypto_type = _get_crypto_type(key_vals)

        if crypto_type == SsoCrypto.DPAPI:
            crypto_obj = _create_windows_dpapi_obj()

        elif crypto_type == SsoCrypto.LINUX_LIBSECRET:
            crypto_obj = _create_linux_libsecret_fernet_obj(key_vals)

        elif crypto_type == SsoCrypto.OSX_KEYCHAIN:
            crypto_obj = _create_osx_keychain_fernet_obj(key_vals)

        elif crypto_type == SsoCrypto.FERNET:
            crypto_obj = _create_fernet_obj(key_vals, SsoCrypto.FERNET)

        if crypto_obj is not None:
            cache_name = _get_cache_name(key_vals)
            gc_ttl_in_secs = options.get('sso_db_gc_interval', 0) * Constants.HOUR_SECS  # convert from hours to seconds
            storage_options = {
                SsoStorageParam.CACHE_SELECTOR_KEY: cache_selector_key,
                SsoStorageParam.CRYPTO_OBJ: crypto_obj,
                SsoStorageParam.CACHE_NAME: cache_name,
                SsoStorageParam.GC_TTL_IN_SECS: gc_ttl_in_secs,
            }

            storage_type = _get_storage_type(key_vals, cache_name)
            if storage_type == SsoStorage.IPYTHON_DB:
                db = IPythonAPI._get_ipython_db(**options)
                sso_storage = DictDbStorage(db, storage_options)
                return sso_storage

    except Exception as ex:
        Display.showWarningMessage(f"{ex}")


def clear_sso_store():
    global sso_storage

    storage = sso_storage or get_sso_store()
    if storage is not None:
        storage.clear_db()


def _get_cache_name(key_vals:dict) -> str:
    cache_name = key_vals.get(SsoEnvVarParam.CACHE_NAME, Constants.SSO_DEFAULT_CACHE_NAME)
    return cache_name


def _get_secret_key(key_vals:dict) -> str:
    secret_key = key_vals.get(SsoEnvVarParam.SECRET_KEY) or str(uuid.uuid4())
    return secret_key


def _get_secret_salt_uuid(key_vals:dict) -> str:
    secret_salt_uuid = key_vals.get(SsoEnvVarParam.SECRET_SALT_UUID) or str(uuid.uuid4())
    return secret_salt_uuid


def _get_crypto_type(key_vals:dict) -> str:
    crypto_type = key_vals.get(SsoEnvVarParam.CRYPTO, SsoCrypto.DEFAULT)
    if crypto_type == SsoCrypto.AUTO:
        if platform.system() == 'Linux':
            crypto_type = SsoCrypto.LINUX_LIBSECRET
        elif platform.system() == 'Darwin':
            crypto_type = SsoCrypto.OSX_KEYCHAIN
        elif platform.system() == 'Windows':
            crypto_type = SsoCrypto.DPAPI
        else:
            crypto_type = "unknown"

    _validate_crypto_type(crypto_type)

    return crypto_type


def _validate_crypto_type(crypto_type:str):
    if crypto_type in _SUPPORTED_SSO_CRYPTO_TYPES:
        pass
    elif crypto_type is None:
        raise SsoWarning(f"Warning: SSO is not activated due to environment variable {Constants.SSO_KEYS_ENV_VAR_NAME} is missing {SsoEnvVarParam.CRYPTO} key/value")
    else:
        raise SsoWarning(f"Warning: SSO is not activated due to {crypto_type} cryptography is not supported")


def _get_storage_type(key_vals:dict, cache_name:str) -> str:
    storage_type = key_vals.get(SsoEnvVarParam.STORAGE, SsoStorage.DEFAULT)
    _validate_storage_type(storage_type, cache_name)
    return storage_type


def _validate_storage_type(storage_type:str, cache_name:str):
    if storage_type in _SUPPORTED_SSO_STORAGE_TYPES:
        pass
    elif storage_type is None:
        raise SsoWarning(f"Warning: SSO is not activated due to environment variable {Constants.SSO_KEYS_ENV_VAR_NAME} is missing {SsoEnvVarParam.STORAGE} key/value")
    else:
        raise SsoWarning(f"Warning: SSO is not activated due to {storage_type} storage is not supported")

    if storage_type == SsoStorage.IPYTHON_DB:
        if not cache_name:
            raise SsoWarning(f"Warning: SSO is not activated due to environment variable {Constants.SSO_KEYS_ENV_VAR_NAME} is missing {SsoEnvVarParam.CACHE_NAME} key/value")


def _create_windows_dpapi_obj() -> DpapiCrypto:
    if platform.system() == 'Windows':
        return DpapiCrypto()
    else:
        raise SsoWarning(f"Warning: SSO is not activated due to {SsoCrypto.DPAPI} cryptography is not supported on {platform.system()} platform")

    
def _create_fernet_obj(key_vals:dict, crypto_key:str) -> FernetCrypto:
    if not FernetCrypto.is_installed():
        raise SsoWarning(f"Warning: SSO is not activated due to {SsoCrypto.FERNET} cryptography and/or password-strength modules are not found")

    if crypto_key is None or crypto_key == SsoCrypto.FERNET:
        secret_key = key_vals.get(SsoEnvVarParam.SECRET_KEY) if crypto_key == SsoCrypto.FERNET else _get_secret_key(key_vals)
        if secret_key is None:
            raise SsoWarning(f"Warning: SSO is not activated due to environment variable {Constants.SSO_KEYS_ENV_VAR_NAME} is missing {SsoEnvVarParam.SECRET_KEY} key/value")

        secret_salt_uuid = _get_secret_salt_uuid(key_vals)
        if secret_salt_uuid is None:
            raise SsoWarning(f"Warning: SSO is not activated due to environment variable {Constants.SSO_KEYS_ENV_VAR_NAME} is missing {SsoEnvVarParam.SECRET_SALT_UUID} key/value")

        hint = FernetCrypto.check_password_strength(secret_key)
        if hint:
            raise SsoWarning(
                f"Warning: SSO could not be activated due to {SsoEnvVarParam.SECRET_KEY} key " 
                f"in environment variable {Constants.SSO_KEYS_ENV_VAR_NAME} is too simple. It should contain: \n{hint}")

        try:
            salt = uuid.UUID(secret_salt_uuid, version=4)            
        except: # pylint: disable=bare-except
            raise SsoWarning(
                f"Warning: SSO is not activated due to {SsoEnvVarParam.SECRET_SALT_UUID} key "
                f"in environment variable {Constants.SSO_KEYS_ENV_VAR_NAME} is not set to a valid uuid")

        cache_name = _get_cache_name(key_vals)
        crypto_options = {
            CryptoParam.PASSWORD: f"{cache_name}-{secret_key}",
            CryptoParam.SALT: salt,
            CryptoParam.LENGTH: 32
        }

    else:
        crypto_options = {
            CryptoParam.CRYPTO_KEY: crypto_key,
        }

    return FernetCrypto(crypto_options)


def _create_osx_keychain_fernet_obj(key_vals:dict) -> FernetCrypto:
    if platform.system() == 'Darwin':
        try:
            from msal_extensions.osx import Keychain, KeychainError
        except: # pylint: disable=bare-except
            raise SsoWarning(f"Warning: SSO is not activated due to {SsoCrypto.OSX_KEYCHAIN} cryptography failed to import Keychain, KeychainError from msal_extensions.osx")
        else:
            cache_name = _get_cache_name(key_vals)
            service_name = Constants.MAGIC_CLASS_NAME_LOWER
            account_name = cache_name
            with Keychain() as locker:
                crypto_key = None
                try:
                    crypto_key = locker.get_generic_password(service_name, account_name)
                except KeychainError as ex:
                    if ex.exit_status != KeychainError.ITEM_NOT_FOUND:
                        raise SsoWarning(
                            f"Warning: SSO is not activated due failure to get key from Keychain, {{serviceName: {service_name}, accountName: {account_name}}}. error: {ex}")
                except Exception as ex:
                    raise SsoWarning(
                        f"Warning: SSO is not activated due failure to get key from Keychain, {{serviceName: {service_name}, accountName: {account_name}}}. error: {ex}")
                else:
                    crypto_obj = _create_fernet_obj(key_vals, crypto_key)
                    if crypto_obj is not None and crypto_key is None:
                        try:
                            locker.set_generic_password(service_name, account_name, crypto_obj.crypto_key)
                        except Exception as ex:
                            raise SsoWarning(
                                f"Warning: SSO is not activated due failure to set key to Keychain, {{serviceName: {service_name}, accountName: {account_name}}}. error: {ex}")
                    return crypto_obj

    else:
        raise SsoWarning(f"Warning: SSO is not activated due to {SsoCrypto.OSX_KEYCHAIN} cryptography is not supported on {platform.system()} platform")



def _create_linux_libsecret_fernet_obj(key_vals:dict) -> FernetCrypto:
    if platform.system() == 'Linux':
        try:
            from msal_extensions.libsecret import LibSecretAgent, trial_run
        except: # pylint: disable=bare-except
            raise SsoWarning(
                f"Warning: SSO is not activated due to {SsoCrypto.LINUX_LIBSECRET} cryptography failed to import LibSecretAgent, trial_run from msal_extensions.libsecret")
        else:
            try:
                trial_run()
            except Exception as ex:
                raise SsoWarning(f"""Warning: SSO is not activated due to libsecret did not perform properly.
* If you encountered error "Remote error from secret service:
  org.freedesktop.DBus.Error.ServiceUnknown",
  you may need to install gnome-keyring package.
* Headless mode (such as in an ssh session) is not supported.
error: {ex}
""")
            else:
                cache_name = _get_cache_name(key_vals)
                schema_name = Constants.MAGIC_CLASS_NAME_LOWER
                attributes = {"sso_cache_name": cache_name}
                try:
                    agent = LibSecretAgent(schema_name, attributes)
                    crypto_key = agent.load()
                except Exception as ex:
                    raise SsoWarning(
                        f"Warning: SSO is not activated due failure to load key from Libsecret, {{schemaName: {schema_name}, attributes: {attributes}}}. error: {ex}")
                else:
                    crypto_obj = _create_fernet_obj(key_vals, crypto_key)
                    if crypto_obj is not None and crypto_key is None:
                        try:
                            agent.save(crypto_obj.crypto_key)
                        except Exception as ex:
                            raise SsoWarning(
                                f"Warning: SSO is not activated due failure to save key in Libsecret, {{schemaName: {schema_name}, attributes: {attributes}}}. error: {ex}")
                    return crypto_obj

    else:
        raise SsoWarning(f"Warning: SSO is not activated due to {SsoCrypto.LINUX_LIBSECRET} cryptography is not supported on {platform.system()} platform")
