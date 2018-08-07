import re
import sys
from nose.tools import with_setup, raises
from kql.kql_magic import Kqlmagic

ip = get_ipython()

class KqlEnv(object):
    # Object constructor
    def __init__(self, connectstr):
        self.connectstr = connectstr
    def query(self, txt):
        return ip.run_line_magic('kql', "%s %s" % (self.connectstr, txt))

kql_env = KqlEnv('$TEST_CONNECTION_STR')

basequery = "let manycoltbl = view () { datatable(name:string, y1:real, y2:real, name2:string, y3:real) ['r1-txt1', 1.01, 1.02, 'r1-txt2', 1.04, 'r2-txt1', 2.01, 2.02, 'r2-txt2', 2.04, 'r3-txt1', 3.01, 3.02, 'r3-txt2', 3.04] }; "

def setup():
    kqlmagic = Kqlmagic(shell=ip)
    ip.register_magics(kqlmagic)

def teardown():
    pass

class Harness(object):
    def run_query(self):
        return kql_env.query(self.query)

class TestOneNum(Harness):
    query = basequery + "manycoltbl | project y1"
    
    @with_setup(setup, teardown)
    def test_pie(self):
        results = self.run_query()
        results.guess_pie_columns(xlabel_sep="//")
        assert results.ys[0].is_quantity
        assert results.ys == [[1.01, 2.01, 3.01]]
        assert results.x == []
        assert results.xlabels == []
        assert results.xlabel == ''

    @with_setup(setup, teardown)
    def test_plot(self):
        results = self.run_query()
        results.guess_plot_columns()
        assert results.ys == [[1.01, 2.01, 3.01]]
        assert results.x == []
        assert results.x.name == ''

class TestOneStrOneNum(Harness):
    query = basequery + "manycoltbl | project name, y1"
    
    @with_setup(setup, teardown)
    def test_pie(self):
        results = self.run_query()
        results.guess_pie_columns(xlabel_sep="//")
        assert results.ys[0].is_quantity
        assert results.ys == [[1.01, 2.01, 3.01]]
        assert results.xlabels == ['r1-txt1', 'r2-txt1', 'r3-txt1']
        assert results.xlabel == 'name'

    @with_setup(setup, teardown)
    def test_plot(self):
        results = self.run_query()
        results.guess_plot_columns()
        assert results.ys == [[1.01, 2.01, 3.01]]
        assert results.x == []


class TestTwoStrTwoNum(Harness):
    query = basequery + "manycoltbl | project name2, y3, name, y1"
    
    @with_setup(setup, teardown)
    def test_pie(self):
        results = self.run_query()
        results.guess_pie_columns(xlabel_sep="//")
        assert results.ys[0].is_quantity
        assert results.ys == [[1.01, 2.01, 3.01]]
        assert results.xlabels == ['r1-txt2//1.04//r1-txt1',
                                  'r2-txt2//2.04//r2-txt1',
                                  'r3-txt2//3.04//r3-txt1']
        assert results.xlabel == 'name2, y3, name'

    @with_setup(setup, teardown)
    def test_plot(self):
        results = self.run_query()
        results.guess_plot_columns()
        assert results.ys == [[1.01, 2.01, 3.01]]
        assert results.x == [1.04, 2.04, 3.04]


class TestTwoStrThreeNum(Harness):
    query = basequery + "manycoltbl | project name, y1, name2, y2, y3"
    
    @with_setup(setup, teardown)
    def test_pie(self):
        results = self.run_query()
        results.guess_pie_columns(xlabel_sep="//")
        assert results.ys[0].is_quantity
        print("1--- {}".format(results.ys))
        print("2--- {}".format([[1.04, 2.04, 3.04]]))
        assert results.ys == [[1.04, 2.04, 3.04]]
        assert results.xlabels == ['r1-txt1//1.01//r1-txt2//1.02',
                                  'r2-txt1//2.01//r2-txt2//2.02',
                                  'r3-txt1//3.01//r3-txt2//3.02']

    @with_setup(setup, teardown)
    def test_plot(self):
        results = self.run_query()
        
        results.guess_plot_columns()
        assert results.ys == [[1.02, 2.02, 3.02], [1.04, 2.04, 3.04]]
        assert results.x == [1.01, 2.01, 3.01]
        
