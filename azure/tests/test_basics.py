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

version_command = "--version"
version_pw_command = version_command + " -pw"
version_expected_pattern = r'Kqlmagic version: [0-9]+\.[0-9]+\.[0-9]+'

@with_setup(_setup, _teardown)
def test_ok():
    assert True

@with_setup(_setup, _teardown)
def test_version():
    result = ip.run_line_magic('kql', version_command)
    version_str = str(result)
    print(version_str)
    expected_pattern = r'^' + version_expected_pattern  + r'$'
    assert re.search(expected_pattern , version_str)

@with_setup(_setup, _teardown)
def test_version_pw_button():
    result = ip.run_line_magic('kql', version_pw_command)
    pw_html_str = result._repr_html_()
    print(pw_html_str)
    assert re.search(r'<button', pw_html_str)
    assert re.search(r'>popup version </button>', pw_html_str)

@with_setup(_setup, _teardown)
def test_version_pw_file():
    result = ip.run_line_magic('kql', version_pw_command)
    pw_html_str = result._repr_html_()
    print(pw_html_str)
    f = re.search(r'kql_MagicLaunchWindowFunction\(\'(.+?)\'\,', pw_html_str)  
    file_path = f.group(1)
    print(file_path)
    version_file = open(file_path) 
    version_html_str = ''
    for line in version_file:
        version_html_str += line
    print(version_html_str)
    expected_pattern = r'^<p>' + version_expected_pattern  + r'</p>$'
    assert re.search(expected_pattern , version_html_str)

# def test_fail():
#     assert False    

@with_setup(_setup, _teardown)
def test_query():
    result = ip.run_line_magic('kql', query1)
    print(result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'
