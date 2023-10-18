#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# because it is also executed from setup.py, make sure
# that it imports only modules, that for sure will exist at setup.py execution
# TRY TO KEEP IMPORTS TO MINIMUM
# --------------------------------------------------------------------------


DESCRIPTION         = "Kqlmagic: Microsoft Azure Monitor magic extension to Jupyter notebook"

NAME                = 'Kqlmagic'

AUTHOR              = 'Michael Binshtock'

AUTHOR_EMAIL        = 'michabin@microsoft.com'

MAINTAINER          = 'Michael Binshtock'

MAINTAINER_EMAIL    = 'michabin@microsoft.com'

URL                 = 'https://github.com/Microsoft/jupyter-Kqlmagic'

PROJECT_URLS        = {
    'Documentation': 'https://github.com/microsoft/jupyter-Kqlmagic/blob/master/README.md',
    'Source': 'https://github.com/microsoft/jupyter-Kqlmagic',
}

LICENSE             = 'MIT License'

KEYWORDS            = 'database ipython jupyter jupyterlab jupyter-notebook nteract azureml query language kql adx azure-data-explorer kusto loganalytics applicationinsights aria'

CLASSIFIERS         = [
    # 'Development Status :: {}'.format(_DEVSTATUS), # will be injected in steup.py based on version_info
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'Intended Audience :: Customer Service',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'Environment :: Console',
    'Environment :: Plugins',
    'License :: OSI Approved :: MIT License',
    'Natural Language :: English',
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
    'Framework :: IPython',
    'Framework :: Jupyter',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3 :: Only',
]

PYTHON_REQUIRES     = '>=3.6'
