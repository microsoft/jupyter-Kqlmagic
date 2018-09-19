
.. image:: https://mybinder.org/badge.svg :target: https://mybinder.org/v2/gh/mbnshtck/jupyter-kql-magic/master?filepath=notebooks%2FQuickStart.ipynb


.. image:: https://mybinder.org/badge.svg :target: https://mybinder.org/v2/gh/mbnshtck/jupyter-kql-magic/master?filepath=notebooks%2FQuickStartAI.ipynb


jupyter-Kqlmagic
===========

Extension (Magic) to Jupyter notebook and Jupyter lab, that enable notebook experience working with Kusto, ApplicationInsights, and LogAnalytics data. 
===========

:Author: Michael Binshtock, mbnshtck@gmail.com

Introduces a %kql (or %%kql) magic.


Connect to kusto, using a connect strings, then issue KQL
commands within IPython or IPython Notebook.

.. image:: https://github.com/mbnshtck/jupyter-kql-magic/master/examples/writers.png
   :width: 600px
   :alt: screenshot of jupyter-kql-magic in the Notebook

Examples
--------

.. code-block:: python

    In [1]: %load_ext kql

    In [2]: %%kql kusto://username('myname').password('mypassword').cluster('mycluster').database('mydatabase')
       ...: character
       ...: | where abbrev = 'ALICE'
       ...:
    Out[2]: [(u'Alice', u'Alice', u'ALICE', u'a lady attending on Princess Katherine', 22)]

    In [3]: result = _

    In [4]: print(result)
    charid   charname   abbrev                description                 speechcount
    =================================================================================
    Alice    Alice      ALICE    a lady attending on Princess Katherine   22

    In [4]: result.columns_name
    Out[5]: [u'charid', u'charname', u'abbrev', u'description', u'speechcount']

    In [6]: result[0][0]
    Out[6]: u'Alice'

    In [7]: result[0].description
    Out[7]: u'a lady attending on Princess Katherine'

After the first connection, connect info can be omitted::

    In [8]: %kql work | count
    Out[8]: [(43L)]

Connections to multiple databases can be maintained.  You can refer to
an existing connection by database@cluster

.. code-block:: python

    In [9]: %%kql mydatabase1@mycluster
       ...: character
       ...: | where  speechcount = (character | summarize max(speechcount))
       ...: | project charname, speechcount
       ...:
    Out[9]: [(u'Poet', 733)]

    In [10]: print(_)
    charname   speechcount
    ======================
    Poet       733

If no connect string is supplied, ``%kql`` will provide a list of existing connections;
however, if no connections have yet been made and the environment variable ``KQLMAGIC_CONNECTION_STR``
is available, that will be used.

For secure access, you may dynamically access your credentials (e.g. from your system environment or `getpass.getpass`) to avoid storing your password in the notebook itself. Use the `$` before any variable to access it in your `%kql` command.

.. code-block:: python

    In [11]: user = os.getenv('SOME_USER')
       ....: password = os.getenv('SOME_PASSWORD')
       ....: connection_string = "kusto://username('{user}'.password('{password}').cluster('some_cluster').database('some_database')".format(user=user, password=password)
       ....: %kql $connection_string
    Out[11]: u'Connected: some_database@some_cluster'

You may use multiple Kql statements inside a single cell, but you will
only see any query results from the last of them, so this really only
makes sense for statements with no output

.. code-block:: python

    In [11]: %%kql
       ....: work | limit 1
       ....: work | count
       ....:
    Out[11]: [(43L)]


Bind variables (bind parameters) can be used in the "named" (:x) style.
The variable names used should be defined in the local namespace

.. code-block:: python

    In [12]: name = 'Countess'

    In [13]: %kql select description from character where charname = :name
    Out[13]: [(u'mother to Bertram',)]

As a convenience, dict-style access for result sets is supported, with the
leftmost column serving as key, for unique values.

.. code-block:: python

    In [14]: result = %kql work

    In [15]: result['richard2']
    Out[15]: (u'richard2', u'Richard II', u'History of Richard II', 1595, u'h', None, u'Moby', 22411, 628)

Results can also be retrieved as an iterator of dictionaries (``result.dicts_iterator()``)
or a single dictionary with a tuple of scalar values per key (``result.to_dict()``)

Assignment
----------

Ordinary IPython assignment works for single-line `%kql` queries:

.. code-block:: python

    In [16]: works = %kql work | project title, year

The `<<` operator captures query results in a local variable, and
can be used in multi-line ``%%kql``:

.. code-block:: python

    In [17]: %%kql works << work
        ...: | project title, year
        ...:
    Returning data to local variable works

Connecting
----------

Some example connection strings::

    kusto://username('username').password('password').cluster('clustername').database('databasename')
    kusto://username('username').password('password').cluster('clustername')
    kusto://username('username').password('password')
    kusto://cluster('clustername').database('databasename')
    kusto://cluster('clustername')
    kusto://database('databasename')


Configuration
-------------

Query results are loaded as lists, so very large result sets may use up
your system's memory and/or hang your browser.  There is no auto_limit
by default.  However, `auto_limit` (if set) limits the size of the result
set (usually with a `LIMIT` clause in the KQL).  `display_limit` is similar,
but the entire result set is still pulled into memory (for later analysis);
only the screen display is truncated.

.. code-block:: python

    In [2]: %config Kqlmagic
    Kqlmagic options
    --------------
    Kqlmagic.auto_limit=<Int>
        Current: 0
        Automatically limit the size of the returned result sets
    Kqlmagic.auto_dataframe=<Bool>
        Current: False
        Return Pandas DataFrames instead of regular result sets
    Kqlmagic.display_limit=<Int>
        Current: 0
        Automatically limit the number of rows displayed (full result set is still
        stored)
    Kqlmagic.feedback=<Bool>
        Current: True
        Print number of records returned, and assigned variables
    Kqlmagic.short_errors=<Bool>
        Current: True
        Don't display the full traceback on KQL Programming Error
    Kqlmagic.prettytable_style=<Unicode>
        Current: 'DEFAULT'
        Set the table printing style to any of prettytable's defined styles
        (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)

    In[3]: %config Kqlmagic.feedback = False

Please note: if you have auto_dataframe set to true, the option will not apply. You can set the pandas display limit by using the pandas ``max_rows`` option as described in the `pandas documentation <http://pandas.pydata.org/pandas-docs/version/0.18.1/options.html#frequently-used-options>`_.

Pandas
------

If you have installed ``pandas``, you can use a result set's
``.DataFrame()`` method

.. code-block:: python

    In [3]: result = %kql character | where speechcount > 25

    In [4]: dataframe = result.DataFrame()

.. _Pandas: http://pandas.pydata.org/

Graphing
--------

If you have installed ``matplotlib``, you can use a result set's
``.plot()``, ``.pie()``, and ``.bar()`` methods for quick plotting

.. code-block:: python

    In[5]: result = %kql work | where genretype = 'c' | project title, totalwords

    In[6]: %matplotlib inline

    In[7]: result.pie()

.. image:: https://raw.github.com/catherinedevlin/ipython-sql/master/examples/wordcount.png
   :alt: pie chart of word count of Shakespeare's comedies

Dumping
-------

Result sets come with a ``.csv(filename=None)`` method.  This generates
comma-separated text either as a return value (if ``filename`` is not
specified) or in a file of the given name.

.. code-block:: python

    In[8]: result = %kql work | where genretype = 'c' | project title, totalwords 

    In[9]: result.csv(filename='work.csv')


Installing
----------

Install the lastest release with::

    pip install jupyter-kql-magic

or download from https://github.com/mbnshtck/jupyter-kql-magic and::

    cd jupyter-kql-magic
    sudo python setup.py install

Development
-----------

https://github.com/mbnshtck/jupyter-kql-magic

Credits
-------

- Kql_
- Kusto_
- Distribute_
- Buildout_
- modern-package-template_

.. _Distribute: http://pypi.python.org/pypi/distribute
.. _Buildout: http://www.buildout.org/
.. _modern-package-template: http://pypi.python.org/pypi/modern-package-template
.. _Kql: https://kusto.azurewebsites.net/docs/queryLanguage/query_language.html
.. _Kusto: https://kusto.azurewebsites.net/docs/
