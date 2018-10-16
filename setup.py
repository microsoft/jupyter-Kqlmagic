#!/usr/bin/env python

#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

"""Setup for Kqlmagic"""

DESCRIPTION         = "Kqlmagic: KQL (Kusto Query Language), enable query Azure Monitor data from a Jupyter compatibe notebook using magic"

NAME                = "Kqlmagic"

AUTHOR              = 'Michael Binshtock'

AUTHOR_EMAIL        = 'michabin@microsoft.com'

MAINTAINER          = 'Michael Binshtock'

MAINTAINER_EMAIL    = 'michabin@microsoft.com'

URL                 = 'https://pypi.python.org/pypi/jupyter-kql-magic'

DOWNLOAD_URL        = 'https://pypi.python.org/pypi/jupyter-kql-magic'

LICENSE             = 'MIT License'

KEYWORDS            = 'database ipython jupyter jupyterlab jupyter-notebook query language kql kusto loganalytics applicationinsights'

INSTALL_REQUIRES    = [
                        'ipython>=7.0.1',
                        'plotly>=3.3.0',
                        'prettytable>=0.7.2',
                        'matplotlib>=3.0.0',
                        'pandas>=0.23.4',
                        'azure-kusto-data>=0.0.15',
                        'azure-kusto-ingest>=0.0.15',
                        'adal>=1.1.0',
                        'Pygments>=2.2.0',
                        'seaborn>=0.9.0',
                        'requests>=2.19.1',
                        'python-dateutil>=2.7.3',
                        'traitlets',
                        'psutil',
                        'six>=1.11.0',
                        'setuptools>=40.4.3',
]


# To use a consistent encoding
import codecs

import re

from os import path

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

CURRENT_PATH = path.abspath(path.dirname(__file__))
PACKAGE_PATH = 'azure-Kqlmagic'.replace('-', path.sep)

with open(path.join(PACKAGE_PATH, 'version.py'), 'r') as fd:
    VERSION = re.search(r'^VERSION\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

CURRENT_PATH = path.abspath(path.dirname(__file__))
with codecs.open(path.join(CURRENT_PATH, 'README.rst'), encoding='utf-8') as f:
    README = f.read()
with codecs.open(path.join(CURRENT_PATH, 'NEWS.txt'), encoding='utf-8') as f:
    NEWS = f.read()
LONG_DESCRIPTION = README + '\n\n' + NEWS



setup(name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    classifiers=[
        'Development Status :: 4 - Beta',
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
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=KEYWORDS,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    maintainer=MAINTAINER,
    maintainer_email=MAINTAINER_EMAIL,
    url=URL,
    download_url=DOWNLOAD_URL,
    license=LICENSE,
    packages=find_packages('azure'),
    package_dir = {'': 'azure'},
    include_package_data=True,
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
)
