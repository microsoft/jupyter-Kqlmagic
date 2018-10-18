Kqlmagic: Microsoft Azure Monitor magic extension to Jupyter notebook
=====================================================================
Enables notebook experience, exploring Microsoft Azure Monitor data: Azure Data Explorer (Kusto),
ApplicationInsights, and LogAnalytics data, from Jupyter notebook (Python3 kernel), using kql (Kusto Query language).

Example
--------

.. code-block:: python

    Install latest version of package
    In [1]: !pip Kqlmagic --no-cache-dir --upgrade

.. code-block:: python

    Add Kqlmagic to notebook magics
    In [2]: %reload_ext Kqlmagic

.. code-block:: python

    Connect to database at cluster
    In [3]: %kql kusto://code;cluster='help';database='Samples'

.. code-block:: python

    Query database@cluster and render result set to a pie chart
    In [4]: %kql Samples@help StormEvents | summarize count() by State | sort by count_ | limit 10 | render piechart title='my apple pie'



Get Started Notebooks
---------------------

* `Get Started with Kqlmagic for Kusto notebook <https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FQuickStart.ipynb>`_.

* `Get Started with Kqlmagic for Application Insights notebook <https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FQuickStartAI.ipynb>`_.

* `Get Started with Kqlmagic for Log Analytics notebook <https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FQuickStartLA.ipynb>`_.


* `Parametrize your Kqlmagic query with Python notebook <https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FParametrizeYourQuery.ipynb>`_.

* `Choose colors palette for your Kqlmagic query chart result notebook <https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FColorYourCharts.ipynb>`_.


Links
-----

* `How to install the package <https://github.com/Microsoft/jupyter-Kqlmagic#install>`_.
* `How to load the magic extension <https://github.com/Microsoft/jupyter-Kqlmagic#load>`_.
* `GitHub Repository <https://github.com/Microsoft/jupyter-Kqlmagic/tree/master>`_.