# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
import json
import threading

from adal.constants import TokenResponseFields

def _string_cmp(str1, str2):
    '''Case insensitive comparison. Return true if both are None'''
    str1 = str1 if str1 is not None else ''
    str2 = str2 if str2 is not None else ''
    return str1.lower() == str2.lower()

class AdalTokenCacheKey(object): # pylint: disable=too-few-public-methods
    def __init__(self, authority, resource, client_id, user_id):
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

# pylint: disable=protected-access

def _get_cache_key(entry):
    return AdalTokenCacheKey(
        entry.get(TokenResponseFields._AUTHORITY), 
        entry.get(TokenResponseFields.RESOURCE), 
        entry.get(TokenResponseFields._CLIENT_ID), 
        entry.get(TokenResponseFields.USER_ID))


class AdalTokenCache(object):
    def __init__(self, state=None):
        ip = get_ipython()  # pylint: disable=E0602
        self.db = ip.db
        self._cache = {}
        self._lock = threading.RLock()
        if state:
            self.deserialize(state)
        else:
            self._restore()
        self.has_state_changed = False


    def find(self, query):
        with self._lock:
            # print("--##-- find token --##--")
            return self._query_cache(
                query.get(TokenResponseFields.IS_MRRT), 
                query.get(TokenResponseFields.USER_ID), 
                query.get(TokenResponseFields._CLIENT_ID))

    def remove(self, entries):
        with self._lock:
            # print("--##-- remove token --##--")
            for e in entries:
                key = _get_cache_key(e)
                removed = self._cache.pop(key, None)
                if removed is not None:
                    self.has_state_changed = True
                    self._save()

    def add(self, entries):
        with self._lock:
            # print("--##-- add token --##--")
            for e in entries:
                key = _get_cache_key(e)
                self._cache[key] = e
            self.has_state_changed = True
            self._save()

    def serialize(self):
        with self._lock:
            # print("--##-- serialize state --##--")
            return json.dumps(list(self._cache.values()))

    def deserialize(self, state):
        with self._lock:
            # print("--##-- deserialize state --##--")
            self._cache.clear()
            if state:
                tokens = json.loads(state)
                for t in tokens:
                    key = _get_cache_key(t)
                    self._cache[key] = t

    def read_items(self):
        '''output list of tuples in (key, authentication-result)'''
        with self._lock:
            # print("--##-- read_items --##--")
            return self._cache.items()

    def _query_cache(self, is_mrrt, user_id, client_id):
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

    def _save(self):
        data = self.serialize()
        self.db['kqlmagicstore/tokens'] = data

    def _restore(self):
        try:
            data = self.db['kqlmagicstore/tokens']
        except KeyError:
            print("no stored tokens")
        else:
            self.deserialize(data)
