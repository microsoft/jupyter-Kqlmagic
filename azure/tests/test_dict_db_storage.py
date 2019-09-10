#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import pytest 
from azure.Kqlmagic.constants import Constants, DpapiParam, SsoCrypto, SsoEnvVarParam, SsoStorage, SsoStorageParam
from azure.Kqlmagic.kql_magic import Kqlmagic as Magic
from azure.Kqlmagic.dict_db_storage import DictDbStorage
from azure.Kqlmagic.dpapi_crypto import DpapiCrypto
from azure.Kqlmagic.fernet_crypto import FernetCrypto,check_password_strength

import os
import string
import random 
import uuid
from datetime import datetime, timedelta

ip = get_ipython() # pylint: disable=E0602

@pytest.fixture 
def dict_db():
    dpapi_obj = DpapiCrypto()
    dict_db = DictDbStorage({}, {SsoStorageParam.CRYPTO_OBJ: dpapi_obj, SsoStorageParam.AUTHORITY:"authority"})
    return dict_db


def get_random_string(length=10):
    """Generate a random string of lowercase letters """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def get_random_string_digits(stringLength=10):
    """Generate a random string of letters, digits and special characters """
    password_characters = string.ascii_letters + string.digits + string.punctuation
    password_characters.replace("'","").replace("`","").replace("\"","")
    return ''.join(random.choice(password_characters) for i in range(stringLength))

def test_ok():
    assert True

#SSOSTORAGE TESTS
def test_get_db_key(dict_db):
    assert dict_db.db_key ==  os.path.join(Constants.SSO_DB_KEY_PREFIX, "sso" + str(os.getlogin()), "authority")


def test_db_save_restore(dict_db):
    dict_db.save("abc")
    assert (dict_db._crypto_obj.decrypt(dict_db.db.get(dict_db.db_key).get("data"))) =="abc"
    assert dict_db.restore() == "abc"

def test_db_bad_save(dict_db):
    dict_db.save("abc")
    dict_db.db.get(dict_db.db_key)["data"] = b'x' #tampering with the token
    assert dict_db.restore() is None

def test_db_clear(dict_db):
    dict_db.save("abc")
    dict_db.clear_db()
    assert dict_db.db =={}


def test_db_garbage_collector(dict_db):
    dict_db.save("def")
    dict_db.db.get(dict_db.db_key)["timestamp"] = datetime.utcnow() +timedelta(seconds=100)
    dict_db._db_gc()
    assert (dict_db._crypto_obj.decrypt(dict_db.db.get(dict_db.db_key).get("data"))) =="def"
    assert dict_db.restore() == "def"

    dict_db.db.get(dict_db.db_key)["timestamp"] = datetime.utcnow() -timedelta(seconds=1000)
    dict_db._db_gc()
    assert dict_db.db =={}

