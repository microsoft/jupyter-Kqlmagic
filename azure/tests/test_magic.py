#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import pytest
from azure.Kqlmagic.constants import Constants
from azure.Kqlmagic.kql_magic import Kqlmagic as Magic
from textwrap import dedent
import os.path
import re
import tempfile

ip = get_ipython() # pylint: disable=undefined-variable

@pytest.fixture 
def register_magic():
    magic = Magic(shell=ip)
    ip.register_magics(magic)


TEST_URI_SCHEMA_NAME = "kusto"

query1 = "-conn=$TEST_CONNECTION_STR let T = view () { datatable(n:long, name:string)[1,'foo',2,'bar'] }; T"

def test_memory_db(register_magic):
    result = ip.run_line_magic('kql', query1)
    print(result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'

def test_html(register_magic):
    result = ip.run_line_magic('kql',  query1)
    print(result)
    html_result = result._repr_html_().lower()
    print(html_result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'
    # assert '<td>foo</td>' in html_result

def test_print(register_magic):
    result = ip.run_line_magic('kql', query1)
    print(result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'
    str_result = str(result)
    print(str_result)
    assert re.search(r'1\s+\|\s+foo', str_result)


def test_plain_style(register_magic):
    ip.run_line_magic('config', "{0}.prettytable_style = 'PLAIN_COLUMNS'".format(Constants.MAGIC_CLASS_NAME))
    result = ip.run_line_magic('kql', query1)
    print(result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'
    str_result = str(result)
    print(str_result)
    assert re.search(r'1\s+\|\s+foo', str(result))

query2 = """
        -conn=$TEST_CONNECTION_STR
        let T = view () { datatable(first_name:string, last_name:string, year_of_death:long)
        ['William', 'Shakespeare', 1616, 'Bertold', 'Brecht', 1956] }; 
        T
        """

def test_multi_sql(register_magic):
    result = ip.run_cell_magic('kql', '', query2)
    print(result)
    str_result = str(result)
    print(str_result)
    assert 'Shakespeare' in str_result and 'Brecht' in str_result

def test_access_results_by_keys(register_magic):
    result = ip.run_line_magic('kql', query2)
    result_by_key = result['William']
    print(result_by_key)
    assert result_by_key == (u'William', u'Shakespeare', 1616)

query4 = """
        -conn=$TEST_CONNECTION_STR
        let T = view () { datatable(first_name:string, last_name:string, year_of_death:long)
        ['William', 'Shakespeare', 1616, 'Bertold', 'Brecht', 1956] }; 
        T | project last_name, last_nameX = last_name
        """

def test_duplicate_column_names_accepted(register_magic):
    result = ip.run_cell_magic('kql', '', query4)
    print(result)
    assert (u'Brecht', u'Brecht') in result

def test_auto_limit(register_magic):
    ip.run_line_magic('config',  "{0}.auto_limit = 0".format(Constants.MAGIC_CLASS_NAME))
    result = ip.run_line_magic('kql',  query1)
    print(result)
    assert len(result) == 2
    ip.run_line_magic('config',  "{0}.auto_limit = 1".format(Constants.MAGIC_CLASS_NAME))
    result = ip.run_line_magic('kql',  query1)
    print(result)
    assert len(result) == 1
    ip.run_line_magic('config',  "{0}.auto_limit = 0".format(Constants.MAGIC_CLASS_NAME))

query6 = "-conn=$TEST_CONNECTION_STR let T = view () { datatable(first_name:string, last_name:string, year_of_death:long)['William', 'Shakespeare', 1616, 'Bertold', 'Brecht', 1956] }; T"

def test_columns_to_local_vars(register_magic):
    ip.run_line_magic('config',  "{0}.columns_to_local_vars = True".format(Constants.MAGIC_CLASS_NAME))
    result = ip.run_line_magic('kql', query6)
    print(result)
    assert result is None
    assert 'William' in ip.user_global_ns['first_name']
    assert 'Shakespeare' in ip.user_global_ns['last_name']
    assert len(ip.user_global_ns['first_name']) == 2
    ip.run_line_magic('config',  "{0}.columns_to_local_vars = False".format(Constants.MAGIC_CLASS_NAME))

def test_userns_not_changed(register_magic):
    ip.run_cell(dedent("""
    def function():
        local_var = 'local_val'
        %kql -conn=$TEST_CONNECTION_STR let T = view () { datatable(first_name:string, last_name:string, year_of_death:long)['William', 'Shakespeare', 1616, 'Bertold', 'Brecht', 1956] }; T
    function()"""))
    assert 'local_var' not in ip.user_ns
    

def test_auto_dataframe(register_magic):
    ip.run_line_magic('config',  "{0}.auto_dataframe = True".format(Constants.MAGIC_CLASS_NAME))
    dframe = ip.run_line_magic('kql', query1)
    # assert dframe.success
    assert 'foo' in str(dframe)

def test_csv(register_magic):
    ip.run_line_magic('config',  "{0}.auto_dataframe = False".format(Constants.MAGIC_CLASS_NAME))  # uh-oh
    result = ip.run_line_magic('kql', query1)
    result = result.to_csv()
    for row in result.splitlines():
        assert row.count(',') == 1
    assert len(result.splitlines()) == 3

def test_csv_to_file(register_magic):
    ip.run_line_magic('config',  "{0}.auto_dataframe = False".format(Constants.MAGIC_CLASS_NAME))  # uh-oh
    result = ip.run_line_magic('kql', query1)
    with tempfile.TemporaryDirectory() as tempdir:
        fname = os.path.join(tempdir, 'test.csv')
        output = result.to_csv(fname)
        assert os.path.exists(output.file_or_image)
        with open(output.file_or_image) as csvfile:
            content = csvfile.read()
            for row in content.splitlines():
                assert row.count(',') == 1
            assert len(content.splitlines()) == 3


def test_dict(register_magic):
    result = ip.run_line_magic('kql',  query6)
    result = result.to_dict()
    assert isinstance(result, dict)
    assert 'first_name' in result
    assert 'last_name' in result
    assert 'year_of_death' in result
    assert len(result['last_name']) == 2

def test_dicts(register_magic):
    result = ip.run_line_magic('kql',  query6)
    for row in result.dicts_iterator():
        assert isinstance(row, dict)
        assert 'first_name' in row
        assert 'last_name' in row
        assert 'year_of_death' in row
