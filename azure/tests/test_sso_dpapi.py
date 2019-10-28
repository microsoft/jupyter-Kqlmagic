#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import pytest
from azure.Kqlmagic.constants import Constants
from azure.Kqlmagic.kql_magic import Kqlmagic as Magic
from textwrap import dedent
import os
import re
from azure.Kqlmagic.constants import Constants, DpapiParam, SsoCrypto, SsoEnvVarParam, SsoStorage, SsoStorageParam
from azure.Kqlmagic.sso_storage import get_sso_store
from azure.Kqlmagic.dpapi_crypto import DpapiCrypto
from azure.Kqlmagic.dict_db_storage import DictDbStorage
from azure.Kqlmagic.connection import Connection
import string
import random
import uuid

ip = get_ipython() # pylint:disable=undefined-variable

#env parameters

def get_random_string(length=10):
    """Generate a random string of lowercase letters """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

cachename = get_random_string(5)

TEST_GUID = "513d7874-4f50-4263-9556-aec382b7180a"

@pytest.fixture 
def register_magic_get_db():
    magic = Magic(shell=ip)
    ip.register_magics(magic)
    dpapi_obj = DpapiCrypto()
    dict_db = DictDbStorage(ip.db, {SsoStorageParam.CRYPTO_OBJ: dpapi_obj, SsoStorageParam.AUTHORITY:"authority"})
    return dict_db



def test_ok(register_magic_get_db, capsys):
    print(cachename)
    config_sso = f"""
    Kqlmagic.sso_encryption_keys="cachename='{cachename}';storage='ipythondb';crypto='dpapi';{TEST_GUID}"
    """    
    ip.run_line_magic('config', config_sso) 
    connection_string = """azureDataExplorer://code;cluster='help';database='Samples' 
    -device_code_login_notification='email'
    -enable_sso=True
    -device_code_notification_email="SMTPEndPoint='smtp-mail.outlook.com';SMTPPort='587';sendFrom='kqlmagic@outlook.com';sendFromPassword='Kql_Magic1';sendTo='kqlmagic@outlook.com'"
    """

    ip.run_cell_magic('kql',"", connection_string) #connecting for first time
    assert True
    # Connection.connections.clear()


def test_sso(register_magic_get_db):
    config_sso = f"""
    Kqlmagic.sso_encryption_keys="cachename='{cachename}';storage='ipythondb';crypto='dpapi';{TEST_GUID}"
    """    
    ip.run_line_magic('config', config_sso) 

    conn_sso = """
    azureDataExplorer://code;cluster='help';database='Samples'
    -enable_sso=True
    """
    query = "StormEvents | summarize count() by State | sort by count_ | limit 10"

    ip.run_cell_magic('kql',"", conn_sso) #connecting for the second time
    result = ip.run_line_magic('kql', query)
    print("Need to respond to email")

    assert result[0][0] == 'TEXAS'
    assert result[0][1] == 4701
    assert result[1][0] == 'KANSAS'
    Connection.connections.clear()
    result = ip.run_line_magic('kql', query) #test_no_sso
    assert result is None


def test_existing_sso(register_magic_get_db): 
    config_sso = f"""
    Kqlmagic.sso_encryption_keys="cachename='{cachename}';storage='ipythondb';crypto='dpapi';{TEST_GUID}"
    """    
    ip.run_line_magic('config', config_sso) 

    query = """
    azureDataExplorer://code;cluster='help';database='Samples'
    -enable_sso=True  StormEvents | summarize count() by State | sort by count_ | limit 10"""

    result = ip.run_cell_magic('kql',"", query)    
    assert result[0][0] == 'TEXAS'
    assert result[0][1] == 4701
    assert result[1][0] == 'KANSAS'



# def test_no_sso(register_magic_get_db, capfd ): #must contain a token
#     Connection.connections.clear()
#     config_sso = """
#     Kqlmagic.sso_encryption_keys="cachename='NOT_EXISTING';storage='ipythondb';crypto='dpapi'"
#     """    
#     ip.run_line_magic('config', config_sso) 

#     query = """
#     azureDataExplorer://code;cluster='help';database='Samples'
#     -enable_sso=True
#     StormEvents | summarize count() by State | sort by count_ | limit 10 """

#     # result  = ip.run_cell_magic('kql',"", conn_sso_no_existing) #connecting for the second time
#     ip.run_cell_magic('kql',"", query)
#     captured = capfd .readouterr()
#     print(captured.out)

#     assert "waiting for response" in captured.out



def test_config_sso_(register_magic_get_db): #should work
    config_sso = f"""
    Kqlmagic.sso_encryption_keys="cachename='{cachename}';storage='ipythondb';crypto='dpapi';{TEST_GUID}"
    """    
    result = ip.run_line_magic('config', config_sso) 
    assert result is None

def test_config_sso_guid(register_magic_get_db, caplog): #should fail
    config_sso = f"""
    Kqlmagic.sso_encryption_keys="cachename='{cachename}';storage='ipythondb';crypto='dpapi';12345"
    """    
    ip.run_line_magic('config', config_sso) 
    assert "sso_encryption_keys cannot be set in config- only via environmental parameters." in caplog.text
