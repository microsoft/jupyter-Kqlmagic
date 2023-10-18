# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from datetime import datetime
import json
import string
import random
import threading


from .dependencies import Dependencies
from .my_utils import json_dumps
from .sso_storage import get_sso_store
from .dict_db_storage import DictDbStorage
from .log import logger


msal = Dependencies.get_module("msal", dont_throw=True)


if msal:
    class MsalTokenCache(msal.SerializableTokenCache):

        @classmethod
        def get_cache(cls, cache_selector_key: str, **options)-> dict:
            "retreive the cache from store, based the selector key"
            store = get_sso_store(cache_selector_key, **options)
            if store:
                cache = MsalTokenCache(store)
                return cache


        def __init__(self, store:DictDbStorage, state:str=None):
            super(MsalTokenCache, self).__init__()
            self._rlock = threading.RLock()

            self._cache_state = super(MsalTokenCache, self).serialize()
            self._store = store
            self.deserialize(state=state)


        # def find(self, credential_type: str, target: list=None, query: dict=None):
        def find(self, *args, **kwargs):
            '''find entries in cache'''
            logger().debug(f"MsalTokenCache find({args}, {kwargs})")

            with self._rlock:
                self.deserialize()
                result = super(MsalTokenCache, self).find(*args, **kwargs)
                return result


        # def modify(self, credential_type: str, old_entry:dict, new_key_value_pairs: dict=None):
        def modify(self, *args, **kwargs):
            '''modify or remove entries in cache'''
            logger().debug(f"MsalTokenCache modify({args}, {kwargs})")

            with self._rlock:
                self.deserialize()
                super(MsalTokenCache, self).modify(*args, **kwargs)
                self.serialize()


        def _random_string(self) -> str:
            return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(random.randint(1, 100)))


        def serialize(self) -> str:
            '''serialize cache'''

            with self._rlock:
                new_cache_state = super(MsalTokenCache, self).serialize()
                if new_cache_state != self._cache_state:
                    self._cache_state = new_cache_state
                    state_obj = {
                        "description": "kqlmagic",
                        "version": 2,
                        "package": "msal",
                        "timestamp": int(datetime.utcnow().timestamp()),
                        "random_string": self._random_string(),  # makes length and content different each time
                        "cache_state": self._cache_state,
                    }
                    state = json_dumps(state_obj)
                    self._store.save(state)
                    return state


        def deserialize(self, state: str=None):
            '''deserialize cache'''

            with self._rlock:
                state = state or self._store.restore()
                if state:
                    state_obj = json.loads(state)
                    if (state_obj.get("description") == "kqlmagic"
                            and state_obj.get("version") == 2
                            and state_obj.get("package") == "msal"
                            and state_obj.get("timestamp") < int(datetime.utcnow().timestamp())
                            and state_obj.get("random_string")
                            and len(state_obj.get("random_string")) >= 1 
                            and len(state_obj.get("random_string")) <= 100 
                            and state_obj.get("cache_state") is not None):
                        self._cache_state = state_obj.get("cache_state")
                        super(MsalTokenCache, self).deserialize(self._cache_state)

else:
    class MsalTokenCache(object):

        @classmethod
        def get_cache(cls, cache_selector_key: str, **options)-> dict:
            return None
