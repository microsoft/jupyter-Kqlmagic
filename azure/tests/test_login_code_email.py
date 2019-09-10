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
from azure.Kqlmagic.fernet_crypto import FernetCrypto,check_password_strength
import uuid

ip = get_ipython() # pylint:disable=undefined-variable

## USE THIS FUNCTIONALITY TO TEST CONNCETIONS TO KUSTO #

#env parameters

@pytest.fixture 
def register_magic():
    print("python path is "+ str(os.getenv("PYTHONPATH")))
    magic = Magic(shell=ip)

    ip.register_magics(magic)


# -conn={conn} 

def test_connection_works(register_magic):
    connection_string = """azureDataExplorer://code;cluster='help';database='Samples' 
    -device_code_login_notification='email' 
    -device_code_notification_email="SMTPEndPoint='smtp-mail.outlook.com';SMTPPort='587';sendFrom='kqlmagic@outlook.com';sendFromPassword='Kql_Magic1';sendTo='kqlmagic@outlook.com'"
    """
    query_no_conn = "StormEvents | summarize count() by State | sort by count_ | limit 10"
    ip.run_line_magic('kql', connection_string)
    result = ip.run_line_magic('kql', query_no_conn)


    assert result[0][0] == 'TEXAS'
    assert result[0][1] == 4701
    assert result[1][0] == 'KANSAS'


