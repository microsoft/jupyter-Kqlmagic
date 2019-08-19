# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
import json
import threading

import time

import base64
import os
from cryptography.fernet import Fernet
from cryptography.fernet import InvalidToken

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from Kqlmagic.log import logger
from Kqlmagic.parser import Parser
from uuid import UUID
from password_strength import PasswordPolicy

from password_strength import tests

from Kqlmagic.display import Display

from Kqlmagic.constants import Constants


from adal.constants import TokenResponseFields
TIME_BETWEEN_CLEANUPS = 3600 #an hour in seconds

def _string_cmp(str1, str2):
    '''Case insensitive comparison. Return true if both are None'''
    str1 = str1 or ''
    str2 = str2 or ''
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
    LAST_CLEAR_TIME = int(time.time())

    def __init__(self, user_id, encryption_key,salt,token_exp_time, state=None):

        ip = get_ipython()  # pylint: disable=undefined-variable
        self.db = ip.db
        self._cache = {}
        self._lock = threading.RLock()


        self.init_fernet(user_id, encryption_key, salt)
        self.token_exp_time = token_exp_time

        if state:
            self.deserialize(state)
        else:
            self._restore()
        self.has_state_changed = False


    def init_fernet(self, user_id, encryption_key, salt):
        self.user_id = user_id
        self.salt  = salt
        self.encryption_key = user_id + encryption_key

        #init fernet
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(),length=34,salt=salt,iterations=100000,backend=default_backend())
        encryption_key_as_bytes = str.encode(self.encryption_key)
        key = kdf.derive(encryption_key_as_bytes)
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(),length=34,salt=salt,iterations=100000,backend=default_backend()) #PBKDF2 instances can only be used once
        kdf.verify(encryption_key_as_bytes, key)
        

        last_two_bytes = [b for b in key[-2:]]
        result = last_two_bytes[0]*256 + last_two_bytes[1]

        self.user_id = user_id + str(result)
        key = key[:-2]
        key = base64.urlsafe_b64encode(key)

        self.key = key
        self.fernet = Fernet(key)

    def find(self, query):
        with self._lock:
            return self._query_cache(
                query.get(TokenResponseFields.IS_MRRT), 
                query.get(TokenResponseFields.USER_ID), 
                query.get(TokenResponseFields._CLIENT_ID))

    def remove(self, entries):
        with self._lock:
            for e in entries:
                key = _get_cache_key(e)
                removed = self._cache.pop(key, None)
                if removed is not None:
                    self.has_state_changed = True
                    self._save()

    def add(self, entries):
        with self._lock:
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

    def clear_db(self):

        items = self.db.items()

        for token_tuple in items:

            db_key = token_tuple[0] # 'kqlmagicstore/tokens/USER_ID' address
            user_id = db_key[len("kqlmagicstore/tokens/"):]

            data = token_tuple[1] #encrypted token
            if not data:
                return
            try:
                time_created, _unused = Fernet._get_unverified_token_data(data)  #get token timestamp 
            except InvalidToken:
                Display.showWarningMessage("Warning: found an illegal token, deleting the token")
                del self.db["kqlmagicstore/tokens/" + user_id ]
            EXP_TIME = self.token_exp_time  
            if(time_created +EXP_TIME < int(time.time()) ):
                del self.db["kqlmagicstore/tokens/" + user_id ]

    def _save(self):
        data = self.serialize()
        data_as_bytes = data.encode()

        #init fernet
        fernet = self.fernet

        #encrypt 
        data_encrypted = fernet.encrypt(data_as_bytes)
        
        self.db['kqlmagicstore/tokens/'+ str(self.user_id)] = data_encrypted

    def _restore(self):

        if(AdalTokenCache.LAST_CLEAR_TIME + TIME_BETWEEN_CLEANUPS < int(time.time()) ):
            AdalTokenCache.LAST_CLEAR_TIME = int(time.time())
            self.clear_db()

        data_encrypted = self.db.get('kqlmagicstore/tokens/'+ str(self.user_id))
        if not data_encrypted:
            return
        fernet = self.fernet
        try:
            data_decrypted_as_bytes = fernet.decrypt(data_encrypted)   
        except InvalidToken: #Either token has bad form or it cannot be decrypted
            try: 
                Fernet._get_unverified_token_data(data_encrypted)
            except: #the token has bad form
                Display.showWarningMessage("Warning: found an illegal token, deleting the token")
                del self.db['kqlmagicstore/tokens/'+ str(self.user_id)]
                return
            Display.showWarningMessage("Invalid token, could not activate Single Sign On") #the token cannot be decrypted
            return
        data = data_decrypted_as_bytes.decode()
        self.deserialize(data)

    
    @staticmethod
    def get_params_SSO(): #pylint: disable=no-method-argument
        SSO_id_enc_key = os.getenv("{0}_SSO_ENCRYPTION_KEYS".format(Constants.MAGIC_CLASS_NAME.upper()))
        key_vals_SSO = Parser.parse_and_get_kv_string(SSO_id_enc_key, {}) if SSO_id_enc_key else {}

        username_SSO = key_vals_SSO.get("username")  
        secret_key_SSO = key_vals_SSO.get("secretkey")
        uuid_salt = key_vals_SSO.get("uuid")
        if uuid_salt and secret_key_SSO:
            try:
                uuid_salt = UUID(uuid_salt, version=4)
            except ValueError:
                raise ValueError("please enter a valid uuid (version 4) for enabling SSO")

            hint = check_password_strength(secret_key_SSO)
            if hint:
                raise ValueError(hint)

        salt_bytes = str(uuid_salt).encode()
    
        if username_SSO and secret_key_SSO and salt_bytes:
            return (True, username_SSO, secret_key_SSO, salt_bytes)
        else:
            return (False, username_SSO, secret_key_SSO, salt_bytes)

def check_password_strength(password):
    password_hints = {
            tests.Length: "please use at least 8 characters.",
            tests.Uppercase: "please use at least one uppercase letter.",
            tests.Numbers: "please use at least 2 digits.",
            tests.NonLetters: "please use at least one non-letter character"
        }
    policy = PasswordPolicy.from_names(
            length=8,  # min length: 8
            uppercase=1,  # need min. 1 uppercase letters
            numbers=2,  # need min. 2 digits
            nonletters=1,  # need min. 2 non-letter characters (digits, specials, anything) 
            )
    results = policy.test(password)
    if len(results)>0:
        hint = "The secret key you have entered is too easy: " + os.linesep 
        for i,policy in enumerate(results,1):
            hint= hint+ str(i)+". "+ password_hints.get(type(policy)) + os.linesep            
        return hint
    return None
    