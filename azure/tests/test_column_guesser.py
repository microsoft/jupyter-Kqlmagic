#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import sys


import pytest


from azure.Kqlmagic.kql_magic import Kqlmagic as Magic


ip = get_ipython() # pylint:disable=undefined-variable



basequery = "let manycoltbl = view () { datatable(name:string, y1:real, y2:real, name2:string, y3:real) ['r1-txt1', 1.01, 1.02, 'r1-txt2', 1.04, 'r2-txt1', 2.01, 2.02, 'r2-txt2', 2.04, 'r3-txt1', 3.01, 3.02, 'r3-txt2', 3.04] }; "
connectstr= '$TEST_CONNECTION_STR'

@pytest.fixture 
def register_magic():
    magic = Magic(shell=ip)
    ip.register_magics(magic)


    
def test_pie(register_magic):
    query = basequery + "manycoltbl | project y1"

    results = ip.run_line_magic('kql', f"-conn={connectstr} {query}")
    results.guess_pie_columns(xlabel_sep="//")
    assert results.ys[0].is_quantity
    assert results.ys == [[1.01, 2.01, 3.01]]
    assert results.x == []
    assert results.xlabels == []
    assert results.xlabel == ''

def test_plot( register_magic):
    query = basequery + "manycoltbl | project y1"

    results = ip.run_line_magic('kql', f"-conn={connectstr} {query}")
    results.guess_plot_columns()
    assert results.ys == [[1.01, 2.01, 3.01]]
    assert results.x == []
    assert results.x.name == ''


def test_pie_TestOneStrOneNum(register_magic):
    query = basequery + "manycoltbl | project name, y1"

    results = ip.run_line_magic('kql', f"-conn={connectstr} {query}")
    results.guess_pie_columns(xlabel_sep="//")
    assert results.ys[0].is_quantity
    assert results.ys == [[1.01, 2.01, 3.01]]
    assert results.xlabels == ['r1-txt1', 'r2-txt1', 'r3-txt1']
    assert results.xlabel == 'name'

def test_plot_TestOneStrOneNum( register_magic):
    query = basequery + "manycoltbl | project name, y1"

    results = ip.run_line_magic('kql', f"-conn={connectstr} {query}")
    results.guess_plot_columns()
    assert results.ys == [[1.01, 2.01, 3.01]]
    assert results.x == []



def test_pie_TestTwoStrTwoNum(register_magic):
    query = basequery + "manycoltbl | project name2, y3, name, y1"

    results = ip.run_line_magic('kql', f"-conn={connectstr} {query}")
    results.guess_pie_columns(xlabel_sep="//")
    assert results.ys[0].is_quantity
    assert results.ys == [[1.01, 2.01, 3.01]]
    assert results.xlabels == ['r1-txt2//1.04//r1-txt1',
                                'r2-txt2//2.04//r2-txt1',
                                'r3-txt2//3.04//r3-txt1']
    assert results.xlabel == 'name2, y3, name'

def test_plot_TestTwoStrTwoNum(register_magic):
    query = basequery + "manycoltbl | project name2, y3, name, y1"

    results = ip.run_line_magic('kql', f"-conn={connectstr} {query}")
    results.guess_plot_columns()
    assert results.ys == [[1.01, 2.01, 3.01]]
    assert results.x == [1.04, 2.04, 3.04]



def test_pie_TestTwoStrThreeNum(register_magic):
    query = basequery + "manycoltbl | project name, y1, name2, y2, y3"

    results = ip.run_line_magic('kql', f"-conn={connectstr} {query}")
    results.guess_pie_columns(xlabel_sep="//")
    assert results.ys[0].is_quantity
    print(f"1--- {results.ys}")
    print(f"2--- {[[1.04, 2.04, 3.04]]}")
    assert results.ys == [[1.04, 2.04, 3.04]]
    assert results.xlabels == ['r1-txt1//1.01//r1-txt2//1.02',
                                'r2-txt1//2.01//r2-txt2//2.02',
                                'r3-txt1//3.01//r3-txt2//3.02']

def test_plot_TestTwoStrThreeNum(register_magic):
    query = basequery + "manycoltbl | project name, y1, name2, y2, y3"

    results = ip.run_line_magic('kql', f"-conn={connectstr} {query}")
    
    results.guess_plot_columns()
    assert results.ys == [[1.02, 2.02, 3.02], [1.04, 2.04, 3.04]]
    assert results.x == [1.01, 2.01, 3.01]
    
