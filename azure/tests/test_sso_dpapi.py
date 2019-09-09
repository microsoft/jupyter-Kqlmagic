#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import pytest
from Kqlmagic.constants import Constants
from Kqlmagic.kql_magic import Kqlmagic as Magic
from textwrap import dedent
import os
import re
from Kqlmagic.constants import Constants, DpapiParam, SsoCrypto, SsoEnvVarParam, SsoStorage, SsoStorageParam
from Kqlmagic.sso_storage import get_sso_store
from Kqlmagic.dpapi_crypto import DpapiCrypto
from Kqlmagic.dict_db_storage import DictDbStorage

import uuid

ip = get_ipython() # pylint:disable=undefined-variable

#env parameters
os.environ['KQLMAGIC_DEVICE_CODE_NOTIFICATION_EMAIL'] = f"SMTPEndPoint='smtp-mail.outlook.com';SMTPPort='587';sendFrom='kqlmagic@outlook.com';sendFromPassword='Kql_Magic1';sendTo='kqlmagic@outlook.com'"
os.environ['KQLMAGIC_SSO_ENCRYPTION_KEYS'] = f"cachename='avital3';storage='ipythondb';crypto='dpapi'"


connection_string = "azureDataExplorer://code;cluster='help';database='Samples' -enable_sso=True -device_code_login_notification='email'"
conn_sso = "azureDataExplorer://code;cluster='help';database='Samples' -enable_sso=True "
query = "StormEvents | summarize count() by State | sort by count_ | limit 10"

@pytest.fixture 
def register_magic_get_db():
    magic = Magic(shell=ip)
    ip.register_magics(magic)
    dpapi_obj = DpapiCrypto()
    dict_db = DictDbStorage(ip.db, {SsoStorageParam.CRYPTO_OBJ: dpapi_obj, SsoStorageParam.AUTHORITY:"authority"})

    return dict_db


def test_ok(register_magic_get_db):
    ip.run_line_magic('kql', connection_string) #connecting for first time
    assert True

def test_sso(register_magic_get_db):
    ip.run_line_magic('kql', conn_sso) #connecting for the second time
    result = ip.run_line_magic('kql', query)
    assert result[0][0] == 'TEXAS'
    assert result[0][1] == 4701
    assert result[1][0] == 'KANSAS'

os.environ['KQLMAGIC_DEVICE_CODE_NOTIFICATION_EMAIL'] =""
os.environ['KQLMAGIC_SSO_ENCRYPTION_KEYS'] = ""
