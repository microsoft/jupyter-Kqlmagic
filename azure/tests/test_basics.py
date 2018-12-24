#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

from nose import with_setup
from nose.tools import raises
from Kqlmagic.constants import Constants
from Kqlmagic.kql_magic import Kqlmagic as Magic
from textwrap import dedent
import os.path
import re
import tempfile

ip = get_ipython() # pylint: disable=E0602

def setup():
    magic = Magic(shell=ip)
    ip.register_magics(magic)

def _setup():
    pass

def _teardown():
    pass

TEST_URI_SCHEMA_NAME = "kusto"

query1 = "-conn=$TEST_CONNECTION_STR let T = view () { datatable(n:long, name:string)[1,'foo',2,'bar'] }; T"

@with_setup(_setup, _teardown)
def test_ok():
    assert True

# def test_fail():
#     assert False    

@with_setup(_setup, _teardown)
def test_query():
    result = ip.run_line_magic('kql', query1)
    # print(result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'
