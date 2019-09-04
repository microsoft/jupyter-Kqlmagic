#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import pytest
from Kqlmagic.constants import Constants, DpapiParam, SsoCrypto, SsoEnvVarParam, SsoStorage, SsoStorageParam
from Kqlmagic.kql_magic import Kqlmagic as Magic
from Kqlmagic.sso_storage import get_sso_store
from Kqlmagic.dpapi_crypto import DpapiCrypto
from Kqlmagic.fernet_crypto import FernetCrypto,check_password_strength

from textwrap import dedent
import os
import re
import tempfile
import string
import random 
import uuid

ip = get_ipython() # pylint: disable=E0602


def get_random_string(length=10):
    """Generate a random string of lowercase letters """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def get_random_string_digits(length = 10):
    """Generate a random password """
    randomSource = string.ascii_letters + string.digits + "[!@$#$%^&*()]-_"
    password = random.choice(string.ascii_lowercase)
    password += random.choice(string.ascii_uppercase)
    password += random.choice(string.digits)
    password += random.choice("[!@$#$%^&*()]-_")
    for i in range(length):
        password += random.choice(randomSource)
    return password


def test_ok():
    assert True

#SSOSTORAGE TESTS
def test_sso_storage_no_env_string_no_authority():
    os.environ[Constants.SSO_ENV_VAR_NAME] =""
    sso_store = get_sso_store()
    assert not sso_store

#TESTING FERNET
def test_sso_storage_no_cachename():
    os.environ[Constants.SSO_ENV_VAR_NAME] = f"{SsoEnvVarParam.CACHE_NAME}='';    {SsoEnvVarParam.SECRET_KEY}='{get_random_string_digits(10)}';     {SsoEnvVarParam.SECRET_SALT_UUID}='{uuid.uuid4()}';    {SsoEnvVarParam.CRYPTO}='fernet';    {SsoEnvVarParam.STORAGE}='ipythondb';"
    sso_store = get_sso_store(authority=get_random_string())
    assert not sso_store 
    os.environ[Constants.SSO_ENV_VAR_NAME] = ""

def test_sso_storage_weak_password():
    os.environ[Constants.SSO_ENV_VAR_NAME] = f"{SsoEnvVarParam.CACHE_NAME}='{get_random_string(5)}';    {SsoEnvVarParam.SECRET_KEY}='12345678';     {SsoEnvVarParam.SECRET_SALT_UUID}='{uuid.uuid4()}';    {SsoEnvVarParam.CRYPTO}='fernet';    {SsoEnvVarParam.STORAGE}='ipythondb';"
    sso_store = get_sso_store(authority=get_random_string())
    assert not sso_store 
    
    os.environ[Constants.SSO_ENV_VAR_NAME] = ""

def test_sso_storage_invalid_uuid():
    os.environ[Constants.SSO_ENV_VAR_NAME] = f"{SsoEnvVarParam.CACHE_NAME}='{get_random_string(5)}';    {SsoEnvVarParam.SECRET_KEY}='{get_random_string_digits(10)}';     {SsoEnvVarParam.SECRET_SALT_UUID}='XXX';    {SsoEnvVarParam.CRYPTO}='fernet';    {SsoEnvVarParam.STORAGE}='ipythondb';"
    sso_store = get_sso_store(authority=get_random_string())
    assert not sso_store 
    os.environ[Constants.SSO_ENV_VAR_NAME] = ""

def test_sso_storage_good_env_string_rand_authority():
    os.environ[Constants.SSO_ENV_VAR_NAME] = f"{SsoEnvVarParam.CACHE_NAME}='{get_random_string(5)}';    {SsoEnvVarParam.SECRET_KEY}='{get_random_string_digits(10)}';     {SsoEnvVarParam.SECRET_SALT_UUID}='{uuid.uuid4()}';    {SsoEnvVarParam.CRYPTO}='fernet';    {SsoEnvVarParam.STORAGE}='ipythondb';"
    print(f"{SsoEnvVarParam.CACHE_NAME}='{get_random_string(5)}';    {SsoEnvVarParam.SECRET_KEY}='{get_random_string_digits(10)}';     {SsoEnvVarParam.SECRET_SALT_UUID}='{uuid.uuid4()}';    {SsoEnvVarParam.CRYPTO}='fernet';    {SsoEnvVarParam.STORAGE}='ipythondb';")
    sso_store = get_sso_store(authority=get_random_string())
    assert sso_store is not None

    os.environ[Constants.SSO_ENV_VAR_NAME] = ""

