# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
import json
import threading
from adal.constants import TokenResponseFields
from .sso_storage import SsoStorage
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
            state = self._store.restore()
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


    def serialize(self):
        '''serialize cache'''
        with self._lock:
            # print("--##-- serialize cache --##--")
            return json.dumps(list(self._cache.values()))


    def deserialize(self, state):
        '''deserialize cache'''
        with self._lock:
            # print("--##-- deserialize state --##--")
            if state:
                self._cache.clear()
                tokens = json.loads(state)
                for t in tokens:
                    key = _get_cache_key(t)
                    self._cache[key] = t


    def read_items(self):
        '''output list of tuples in (key, authentication-result)'''
        with self._lock:
            # print("--##-- read_items --##--")
            state = self._store.restore()
            self.deserialize(state)
            return self._cache.items()


    def _query_cache(self, is_mrrt, user_id, client_id):
        '''query cache for matches'''
        # print("--##-- query --##--")
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

