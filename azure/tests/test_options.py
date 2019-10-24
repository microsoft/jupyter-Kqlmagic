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




query1 = "-conn=$TEST_CONNECTION_STR let T = view () { datatable(n:long, name:string)[1,'foo',2,'bar'] }; T"
def test_auto_dataframe(register_magic):
    ip.run_line_magic('config',  "{0}.auto_dataframe = True".format(Constants.MAGIC_CLASS_NAME))
    dframe = ip.run_line_magic('kql', query1)
    # assert dframe.success
    assert 'foo' in str(dframe)
    ip.run_line_magic('config',  "{0}.auto_dataframe = False".format(Constants.MAGIC_CLASS_NAME))



query2 = "-conn=$TEST_CONNECTION_STR pageViews | where client_City != '' | summarize count() by client_City | sort by count_ | limit 10"

def test_feedback(register_magic):
    ip.run_line_magic('config',  "{0}.feedback = True".format(Constants.MAGIC_CLASS_NAME))
    result = ip.run_line_magic('kql', query2)
    assert result.feedback_info is not None
    print(result.feedback_info)
    ip.run_line_magic('config',  "{0}.feedback = False".format(Constants.MAGIC_CLASS_NAME))
    result = ip.run_line_magic('kql', query2)
    print(result.feedback_info)
    assert not result.feedback_info
connection_string = "appinsights://appid='DEMO_APP';appkey='DEMO_KEY'"
query_no_conn = " pageViews | where client_City != '' | summarize count() by client_City | sort by count_ | limit 10"

def test_show_conn_info(register_magic): #need to test list
    ip.run_line_magic('config',  "{0}.show_conn_info = 'None'".format(Constants.MAGIC_CLASS_NAME))
    ip.run_line_magic('kql', connection_string)
    result = ip.run_line_magic('kql', query_no_conn)
    
    print(result.metadata["conn_info"])
    assert not result.metadata["conn_info"]
    ip.run_line_magic('config',  "{0}.show_conn_info = 'current'".format(Constants.MAGIC_CLASS_NAME))
    ip.run_line_magic('kql', connection_string)
    result = ip.run_line_magic('kql', query_no_conn)
    print(result.metadata["conn_info"])
    assert result.metadata["conn_info"]==[' * DEMO_APP@applicationinsights']

def test_show_query_time(register_magic):
    result = ip.run_line_magic('kql', query2)
    assert result.feedback_info is not None
    print(result.feedback_info)
    ip.run_line_magic('config',  "{0}.show_query_time = False".format(Constants.MAGIC_CLASS_NAME))
    result = ip.run_line_magic('kql', query2)
    print(result.feedback_info)
    assert not result.feedback_info


def test_auto_limit(register_magic):
    ip.run_line_magic('config',  "{0}.auto_limit = 0".format(Constants.MAGIC_CLASS_NAME))
    result = ip.run_line_magic('kql',  query2)
    print(result)
    assert len(result) == 10
    ip.run_line_magic('config',  "{0}.auto_limit = 1".format(Constants.MAGIC_CLASS_NAME))
    result = ip.run_line_magic('kql',  query2)
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

