# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, Dict
import os
from datetime import datetime, timedelta


from .display import Display
from .constants import Constants, SsoStorageParam
from .log import logger


class DictDbStorage(object):

    last_clear_time = None

    def __init__(self, db:Dict[str,str], sso_options:Dict[str,Any], state:str=None)->None:
        self.cache_selector_key = sso_options.get(SsoStorageParam.CACHE_SELECTOR_KEY)
        self.db = db or {}
        self.gc_ttl_in_secs = sso_options.get(SsoStorageParam.GC_TTL_IN_SECS, 0)
        DictDbStorage.last_clear_time = DictDbStorage.last_clear_time or (datetime.utcnow() - timedelta(seconds=self.gc_ttl_in_secs))

        self._crypto_obj = sso_options.get(SsoStorageParam.CRYPTO_OBJ)
        self._db_key_prefix = self._get_db_key_prefix(sso_options)
        self.db_key = self._get_db_key(self._db_key_prefix, self.cache_selector_key)

        self.db_key_conflict = False
        
        self.restore()  # to throw warnings\errors 
        if state:
            self.save(state)


    def _get_db_key_prefix(self, sso_options:Dict[str,Any])->str:
        """key_prefix: kqlmagic_store/tokens/<cache_name>/<user_name | unique_key_string>"""

        path_args = [
            Constants.SSO_DB_KEY_PREFIX,
            sso_options.get(SsoStorageParam.CACHE_NAME, Constants.SSO_DEFAULT_CACHE_NAME)
        ]
        crypto_obj = sso_options.get(SsoStorageParam.CRYPTO_OBJ)
        if crypto_obj and crypto_obj.suffix:
            path_args.append(crypto_obj.suffix)
        db_key_prefix = os.path.join(*path_args)
        return db_key_prefix


    def _get_db_key(self, db_key_prefix:str, cache_selector_key:str)->str:
        """key: kqlmagic_store/tokens/<cache_name>/<user_name | unique_key_string>/<cache_selector>"""

        if cache_selector_key:
            idx = cache_selector_key.find("://")
            sso_cache_selector_key = cache_selector_key[0 if idx < 0 else idx + 3:]
            db_key = os.path.join(db_key_prefix, sso_cache_selector_key)
        else:
            db_key = db_key_prefix
        return db_key


    def _db_gc(self)->None:
        """db garbage collector. remove old entries"""

        logger().debug(f"DictDbStorage::_db_gc ")
        for db_key, db_value in self.db.items():
            logger().debug(f"DictDbStorage::_db_gc db_key, db_value in self.db.items() {db_key}, {db_value} in self.db.items() ")

            if db_key.startswith(Constants.SSO_DB_KEY_PREFIX):
                state_encrypted = db_value
                if not state_encrypted:
                    continue
                time_created = state_encrypted.get("timestamp")
                logger().debug(f"DictDbStorage::time_created {time_created}  timedelta(seconds=self.gc_ttl_in_secs) {timedelta(seconds=self.gc_ttl_in_secs)}")

                if (time_created + timedelta(seconds=self.gc_ttl_in_secs)) < datetime.utcnow():
                    del self.db[db_key]


    def clear_db(self)->None:
        """clear db. remove all entries"""

        for db_key in self.db.keys():
            logger().debug(f"DictDbStorage::clear_db - check whether db_key: {db_key} startswith {self._db_key_prefix}")

            if db_key.startswith(self._db_key_prefix):
                logger().debug(f"DictDbStorage::clear_db - db_key: {db_key} (startswith {self._db_key_prefix})")
                del self.db[db_key]


    def save(self, state:str)->None:
        """save cache state in db"""

        logger().debug(f"DictDbStorage::save(self, state: str) -> None state is {type(state)}")

        if not self.db_key_conflict:
            state_encrypted = self._crypto_obj.encrypt(state)  
            self.db[self.db_key] = {'data': state_encrypted, 'timestamp': datetime.utcnow()}


    def restore(self)->str:
        """restore cache state from db"""

        logger().debug(f"DictDbStorage::restore(self) -> str")
        
        if self.gc_ttl_in_secs > 0 and (self.last_clear_time + timedelta(seconds=self.gc_ttl_in_secs)) < datetime.utcnow():
            self.last_clear_time = datetime.utcnow()
            self._db_gc()

        if self.db_key_conflict:
            return
        
        state_encrypted = self.db.get(self.db_key)
        if not state_encrypted:
            return

        try:
            value = self._crypto_obj.decrypt(state_encrypted.get("data"))
            return value

        except: # pylint: disable=bare-except
            try: 
                self.db_key_conflict = True

                self._crypto_obj.verify(state_encrypted) 
                Display.showWarningMessage("Warning: SSO disabled, due to cache_name conflict")

            except: # pylint: disable=bare-except
                Display.showWarningMessage("Warning: SSO disabled, due to cache_name conflict (invalid data in db)")
                # del self.db[self.db_key]
