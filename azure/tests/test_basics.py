#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import os.path
import re
import tempfile
from textwrap import dedent


import pytest


from azure.Kqlmagic.constants import Constants
from azure.Kqlmagic.kql_magic import Kqlmagic as Magic


ip = get_ipython() # pylint:disable=undefined-variable


@pytest.fixture 
def register_magic():
    magic = Magic(shell=ip)
    ip.register_magics(magic)

TEST_URI_SCHEMA_NAME = "kusto"
query1 = "-conn=$TEST_CONNECTION_STR let T = view () { datatable(n:long, name:string)[1,'foo',2,'bar'] }; T"
query2 = "-conn=$TEST_CONNECTION_STR pageViews | where client_City != '' | summarize count() by client_City | sort by count_ | limit 10"

version_command = "--version"
version_pw_command = version_command + " -pw"
version_expected_pattern = r'Kqlmagic version: [0-9]+\.[0-9]+\.[0-9]+\.[\w]+'

def test_ok(register_magic):
    assert True


#Testing "--version" command
def test_version(register_magic):
    result = ip.run_line_magic('kql', version_command)
    version_str = str(result)
    print(version_str)
    expected_pattern = r'^' + version_expected_pattern  + r'$'
    assert re.search(expected_pattern , version_str)


#Testing "--version" command with pop up window button
def test_version_pw_button(register_magic):
    result = ip.run_line_magic('kql', version_pw_command)
    pw_html_str = result._repr_html_()
    print(pw_html_str)
    assert re.search(r'<button', pw_html_str)
    assert re.search(r'>popup version </button>', pw_html_str)

#Testing "--version" command with pop up window button
def test_version_pw_file(register_magic):
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


# This method creates a table with query 1 and checks its components. The table created is:
# +---+------+
# | n | name |
# +---+------+
# | 1 | foo  |
# | 2 | bar  |

def test_query(register_magic):
    result = ip.run_line_magic('kql', query1)
    print(result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'



# This method sends an example query:
# "pageViews | where client_City != '' | summarize count() by client_City | sort by count_ | limit 10"
# and checks the result
def test_query2(register_magic):
    result = ip.run_line_magic('kql', query2)

    print(result)
    assert result[0][0] == "Bothell"
    assert result[1][0] == "Peterborough"

