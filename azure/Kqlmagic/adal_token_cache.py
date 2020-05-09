# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from datetime import datetime, timedelta
import json
import string
import random
import threading


from adal.constants import TokenResponseFields


from .my_utils import json_dumps
from .sso_storage import SsoStorage, get_sso_store
from .log import logger


def _string_cmp(str1, str2):
    '''Case insensitive comparison. Return true if both are None'''
    str1 = str1 or ''
    str2 = str2 or ''
    return str1.lower() == str2.lower()


# pylint: disable=too-few-public-methods
class AdalTokenCacheKey(object): 

    def __init__(self, authority, resource, client_id, user_id):
        logger().debug(f"AdalTokenCacheKey authority {authority} client_id {client_id}  user_id {user_id} ")
        
        self.authority = authority
        self.resource = resource
        self.client_id = client_id
        self.user_id = user_id


    def __hash__(self):
        return hash((self.authority, self.resource, self.client_id, self.user_id))


    def __eq__(self, other):
        return _string_cmp(self.authority, other.authority) and \
               _string_cmp(self.resource, other.resource) and \
               _string_cmp(self.client_id, other.client_id) and \
               _string_cmp(self.user_id, other.user_id)


    def __ne__(self, other):
        return not self == other
# pylint: enable=too-few-public-methods


# pylint: disable=protected-access
def _get_cache_key(entry):
    return AdalTokenCacheKey(
        entry.get(TokenResponseFields._AUTHORITY), 
        entry.get(TokenResponseFields.RESOURCE), 
        entry.get(TokenResponseFields._CLIENT_ID), 
        entry.get(TokenResponseFields.USER_ID))
# pylint: enable=protected-access


class AdalTokenCache(object):

    @classmethod
    def get_cache(cls, authority_key, **options):
        store = get_sso_store(authority_key, **options)
        if store:
            cache = AdalTokenCache(store)
            return cache


    def __init__(self, store: SsoStorage, state=None):
        self._lock = threading.RLock()

        self._cache = {}
        self._store = store
        if state:
            self.deserialize(state)
        self.has_state_changed = False


    def find(self, query):
        logger().debug(f"AdalTokenCache find(self, query)")

        '''find entries in cache'''
        with self._lock:
            state = self._store.restore()
            self.deserialize(state)
            return self._query_cache(
                query.get(TokenResponseFields.IS_MRRT), 
                query.get(TokenResponseFields.USER_ID), 
                query.get(TokenResponseFields._CLIENT_ID)
            )


    def remove(self, entries):
        logger().debug(f"AdalTokenCache remove(self, query)")

        '''remove entries from cache'''
        with self._lock:
            state = self._store.restore()
            self.deserialize(state)
            removed = None
            for e in entries:
                key = _get_cache_key(e)
                removed = self._cache.pop(key, None) or removed
            if removed:
                self.has_state_changed = True
                state = self.serialize()
                self._store.save(state)


    def add(self, entries):
        logger().debug(f"AdalTokenCache add(self, entries")

        '''add entries to cache'''
        with self._lock:
            state: str = self._store.restore()
            self.deserialize(state)
            added = None
            for e in entries:
                key = _get_cache_key(e)
                self._cache[key] = e
                added = True
            if added:
                self.has_state_changed = True
                state = self.serialize()
                self._store.save(state)


    def _random_string(self) -> str:
        return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(random.randint(1, 100)))


    def serialize(self) -> str:
        '''serialize cache'''
        with self._lock:
            state_obj = {
                "description": "kqlmagic",
                "version": 1,
                "timestamp": int(datetime.utcnow().timestamp()),
                "random_string": self._random_string(), # makes length and content different each time
                "cache_values": list(self._cache.values()),
            }
            # print(f">>> --##-- serialize cache --##--")
            return json_dumps(state_obj)


    def deserialize(self, state: str):
        '''deserialize cache'''
        with self._lock:
            # print(f">>> --##-- deserialize state --##--")
            if state:
                self._cache.clear()
                state_obj = json.loads(state)
                if      state_obj.get("description") == "kqlmagic" and \
                        state_obj.get("version") == 1 and \
                        state_obj.get("timestamp") < int(datetime.utcnow().timestamp()) and \
                        state_obj.get("random_string") and len(state_obj.get("random_string")) >= 1 and len(state_obj.get("random_string")) <= 100 and \
                        state_obj.get("cache_values") :
                    cache_values = state_obj["cache_values"]
                    for val in cache_values:
                        key = _get_cache_key(val)
                        self._cache[key] = val


    def read_items(self):
        '''output list of tuples in (key, authentication-result)'''
        with self._lock:
            # print(f">>> --##-- read_items --##--")
            state = self._store.restore()
            self.deserialize(state)
            return self._cache.items()


    def _query_cache(self, is_mrrt, user_id, client_id):
        '''query cache for matches'''
        # print(f">>> --##-- query --##--")
        matches = []
        for k in self._cache:
            v = self._cache[k]
            #None value will be taken as wildcard match
            #pylint: disable=too-many-boolean-expressions
            if ((is_mrrt is None or is_mrrt == v.get(TokenResponseFields.IS_MRRT)) and 
                    (user_id is None or _string_cmp(user_id, v.get(TokenResponseFields.USER_ID))) and 
                    (client_id is None or _string_cmp(client_id, v.get(TokenResponseFields._CLIENT_ID)))):
                matches.append(v)
        return matches

