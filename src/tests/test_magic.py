#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

from nose import with_setup
from nose.tools import raises
from kql.kql_magic import Kqlmagic
from textwrap import dedent
import os.path
import re
import tempfile

ip = get_ipython()

def setup():
    kqlmagic = Kqlmagic(shell=ip)
    ip.register_magics(kqlmagic)

def _setup():
    pass

def _teardown():
    pass

query1 = "$TEST_CONNECTION_STR let T = view () { datatable(n:long, name:string)[1,'foo',2,'bar'] }; T"

@with_setup(_setup, _teardown)
def test_memory_db():
    result = ip.run_line_magic('kql', query1)
    print(result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'

@with_setup(_setup, _teardown)
def test_html():
    result = ip.run_line_magic('kql',  query1)
    print(result)
    html_result = result._repr_html_().lower()
    print(html_result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'
    assert '<td>foo</td>' in html_result

@with_setup(_setup, _teardown)
def test_print():
    result = ip.run_line_magic('kql', query1)
    print(result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'
    str_result = str(result)
    print(str_result)
    assert re.search(r'1\s+\|\s+foo', str_result)


@with_setup(_setup, _teardown)
def test_plain_style():
    ip.run_line_magic('config', "Kqlmagic.prettytable_style = 'PLAIN_COLUMNS'")
    result = ip.run_line_magic('kql', query1)
    print(result)
    assert result[0][0] == 1
    assert result[1]['name'] == 'bar'
    str_result = str(result)
    print(str_result)
    assert re.search(r'1\s+\|\s+foo', str(result))

query2 = """
        $TEST_CONNECTION_STR
        let T = view () { datatable(first_name:string, last_name:string, year_of_death:long)
        ['William', 'Shakespeare', 1616, 'Bertold', 'Brecht', 1956] }; 
        T
        """

@with_setup(_setup, _teardown)
def test_multi_sql():
    result = ip.run_cell_magic('kql', '', query2)
    print(result)
    str_result = str(result)
    print(str_result)
    assert 'Shakespeare' in str_result and 'Brecht' in str_result

query3 = """
        $TEST_CONNECTION_STR
        x <<
        let T = view () { datatable(first_name:string, last_name:string, year_of_death:long)
        ['William', 'Shakespeare', 1616, 'Bertold', 'Brecht', 1956] }; 
        T
        """

@with_setup(_setup, _teardown)
def test_result_var():
    result = ip.run_cell_magic('kql', '', query3)
    print(result)
    x_result = ip.user_global_ns['x']
    print(x_result)
    assert 'Shakespeare' in str(x_result) and 'Brecht' in str(x_result)

@with_setup(_setup, _teardown)
def test_access_results_by_keys():
    result = ip.run_line_magic('kql', query2)
    result_by_key = result['William']
    print(result_by_key)
    assert result_by_key == (u'William', u'Shakespeare', 1616)

query4 = """
        $TEST_CONNECTION_STR
        let T = view () { datatable(first_name:string, last_name:string, year_of_death:long)
        ['William', 'Shakespeare', 1616, 'Bertold', 'Brecht', 1956] }; 
        T | project last_name, last_nameX = last_name
        """

@with_setup(_setup, _teardown)
def test_duplicate_column_names_accepted():
    result = ip.run_cell_magic('kql', '', query4)
    print(result)
    assert (u'Brecht', u'Brecht') in result

@with_setup(_setup, _teardown)
def test_auto_limit():
    ip.run_line_magic('config',  "Kqlmagic.auto_limit = 0")
    result = ip.run_line_magic('kql',  query1)
    print(result)
    assert len(result) == 2
    ip.run_line_magic('config',  "Kqlmagic.auto_limit = 1")
    result = ip.run_line_magic('kql',  query1)
    print(result)
    assert len(result) == 1

query5 = """
        $TEST_CONNECTION_STR
        let T = view () { datatable(Result:string)
        ['apple', 'banana', 'cherry'] }; 
        T
        | sort by Result asc
        """
def test_display_limit():
    ip.run_line_magic('config',  "Kqlmagic.auto_limit = None")
    ip.run_line_magic('config',  "Kqlmagic.display_limit = None")
    result = ip.run_line_magic('kql', query5)
    print(result)
    assert 'apple' in result._repr_html_()
    assert 'banana' in result._repr_html_()
    assert 'cherry' in result._repr_html_()
    ip.run_line_magic('config',  "Kqlmagic.display_limit = 1")
    assert 'apple' in result._repr_html_()
    assert 'cherry' not in result._repr_html_()

query6 = "$TEST_CONNECTION_STR let T = view () { datatable(first_name:string, last_name:string, year_of_death:long)['William', 'Shakespeare', 1616, 'Bertold', 'Brecht', 1956] }; T"

@with_setup(_setup, _teardown)
def test_columns_to_local_vars():
    ip.run_line_magic('config',  "Kqlmagic.columns_to_local_vars = True")
    result = ip.run_line_magic('kql', query6)
    print(result)
    assert result is None
    assert 'William' in ip.user_global_ns['first_name']
    assert 'Shakespeare' in ip.user_global_ns['last_name']
    assert len(ip.user_global_ns['first_name']) == 2
    ip.run_line_magic('config',  "Kqlmagic.columns_to_local_vars = False")

@with_setup(_setup, _teardown)
def test_userns_not_changed():
    ip.run_cell(dedent("""
    def function():
        local_var = 'local_val'
        %kql $TEST_CONNECTION_STR let T = view () { datatable(first_name:string, last_name:string, year_of_death:long)['William', 'Shakespeare', 1616, 'Bertold', 'Brecht', 1956] }; T
    function()"""))
    assert 'local_var' not in ip.user_ns

    """
def test_bind_vars():
    ip.user_global_ns['x'] = 22
    result = ip.run_line_magic('kql', "kusto:// SELECT :x")
    assert result[0][0] == 22
    """

@with_setup(_setup, _teardown)
def test_auto_dataframe():
    ip.run_line_magic('config',  "Kqlmagic.auto_dataframe = True")
    dframe = ip.run_cell("%kql {0}".format(query1))
    assert dframe.success
    assert dframe.result.name[0] == 'foo'

@with_setup(_setup, _teardown)
def test_csv():
    ip.run_line_magic('config',  "Kqlmagic.auto_dataframe = False")  # uh-oh
    result = ip.run_line_magic('kql', query1)
    result = result.csv()
    for row in result.splitlines():
        assert row.count(',') == 1
    assert len(result.splitlines()) == 3

@with_setup(_setup, _teardown)
def test_csv_to_file():
    ip.run_line_magic('config',  "Kqlmagic.auto_dataframe = False")  # uh-oh
    result = ip.run_line_magic('kql', query1)
    with tempfile.TemporaryDirectory() as tempdir:
        fname = os.path.join(tempdir, 'test.csv')
        output = result.csv(fname)
        assert os.path.exists(output.file_path)
        with open(output.file_path) as csvfile:
            content = csvfile.read()
            for row in content.splitlines():
                assert row.count(',') == 1
            assert len(content.splitlines()) == 3


@with_setup(_setup, _teardown)
def test_dict():
    result = ip.run_line_magic('kql',  query6)
    result = result.to_dict()
    assert isinstance(result, dict)
    assert 'first_name' in result
    assert 'last_name' in result
    assert 'year_of_death' in result
    assert len(result['last_name']) == 2

@with_setup(_setup, _teardown)
def test_dicts():
    result = ip.run_line_magic('kql',  query6)
    for row in result.dicts_iterator():
        assert isinstance(row, dict)
        assert 'first_name' in row
        assert 'last_name' in row
        assert 'year_of_death' in row
