#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import pytest
from azure.Kqlmagic.constants import DpapiParam, SsoCrypto, SsoEnvVarParam, SsoStorage, SsoStorageParam
from azure.Kqlmagic.kql_magic import Kqlmagic as Magic
from azure.Kqlmagic.dpapi_crypto import DpapiCrypto
from azure.Kqlmagic.fernet_crypto import FernetCrypto,check_password_strength, generate_key

import os
import string
import random 
import uuid


def get_random_string(length=10):
    """Generate a random string of lowercase letters """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

# def get_random_string_digits(length=10):
#     """Generate a random string of letters and digits """
#     lettersAndDigits = string.ascii_letters + string.digits + string.ascii_uppercase
#     return ''.join(random.choice(lettersAndDigits) for i in range(length))

def get_random_string_digits(length = 10):
    """Generate a random password """
    randomSource = string.ascii_letters + string.digits + "[!@$#$%^&*()]-_"
    password = random.choice(string.ascii_lowercase)
    password += random.choice(string.ascii_uppercase)
    password += random.choice(string.digits)
    password += random.choice(string.digits)
    password += random.choice(string.punctuation)
    for i in range(length):
        password += random.choice(randomSource)
    return password

def test_ok():
    assert True


#DPAPI TESTS
def test_crypto_dpapi_no_params():
    dpapi_obj = DpapiCrypto()
    assert dpapi_obj.suffix == os.getlogin()
    secret=  get_random_string()
    assert dpapi_obj.decrypt(dpapi_obj.encrypt(secret)) == secret 

def test_crypto_dpapi_params():
    options = {DpapiParam.DESCRIPTION:get_random_string(20), DpapiParam.SALT:get_random_string(15)}
    dpapi_obj = DpapiCrypto(options = options)
    assert dpapi_obj.suffix == os.getlogin()
    secret=  get_random_string()
    assert dpapi_obj.decrypt(dpapi_obj.encrypt(secret)) == secret 

#FERNET TESTS

def test_crypto_fernet_params():
    options_fernet= {SsoEnvVarParam.CACHE_NAME: get_random_string(5), SsoEnvVarParam.SECRET_KEY :get_random_string(15), SsoEnvVarParam.SECRET_SALT_UUID: uuid.uuid4()}
    fernet_obj = FernetCrypto(options = options_fernet)
    secret=  get_random_string()
    assert fernet_obj.decrypt(fernet_obj.encrypt(secret)) == secret 

def test_crypto_fernet_params_weak_password():
    options_fernet= {SsoEnvVarParam.CACHE_NAME: get_random_string(5), SsoEnvVarParam.SECRET_KEY :"12345678", SsoEnvVarParam.SECRET_SALT_UUID: uuid.uuid4()}
    secret_key = options_fernet.get(SsoEnvVarParam.SECRET_KEY)
    assert secret_key == "12345678"
    hint = check_password_strength(secret_key)
    assert hint is not None

def test_crypto_fernet_generate_key():
    key = generate_key()
    options_fernet= {SsoEnvVarParam.CACHE_NAME: get_random_string(5), SsoEnvVarParam.ENCRYPT_KEY: key}
    fernet_obj = FernetCrypto(options = options_fernet)
    assert fernet_obj.decrypt(fernet_obj.encrypt("abc"))=="abc"


def test_crypto_fernet_params_strong_password():
    options_fernet= {SsoEnvVarParam.CACHE_NAME: get_random_string(5), SsoEnvVarParam.SECRET_KEY :get_random_string_digits(20), SsoEnvVarParam.SECRET_SALT_UUID: uuid.uuid4()}
    secret_key = options_fernet.get(SsoEnvVarParam.SECRET_KEY)
    assert secret_key is not None
    hint = check_password_strength(secret_key)
    assert hint is None


