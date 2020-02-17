#!/usr/bin/env python

#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

"""Setup for Kqlmagic"""

# To use a consistent encoding
import codecs
import sys
import re
from os import path


# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


DESCRIPTION         = "Kqlmagic: Microsoft Azure Monitor magic extension to Jupyter notebook"

NAME                = "Kqlmagic"

AUTHOR              = 'Michael Binshtock'

AUTHOR_EMAIL        = 'michabin@microsoft.com'

MAINTAINER          = 'Michael Binshtock'

MAINTAINER_EMAIL    = 'michabin@microsoft.com'

URL                 = 'https://github.com/Microsoft/jupyter-Kqlmagic'

LICENSE             = 'MIT License'

KEYWORDS            = 'database ipython jupyter jupyterlab jupyter-notebook query language kql kusto loganalytics applicationinsights'

INSTALL_REQUIRES    = [
                        'ipython>=7.1.1',
                        'ipykernel>=5.1.1',
                        'plotly>=3.10.0',
                        'prettytable>=0.7.2',
                        'matplotlib>=3.0.0',
                        'pandas>=0.23.4',
                        'adal>=1.2.1',
                        'Pygments>=2.2.0',
                        'seaborn>=0.9.0',
                        'requests>=2.21.0',
                        'python-dateutil>=2.7.5',
                        'traitlets>=4.3.2',
                        'psutil>=5.4.7',
                        'six>=1.11.0',
                        'setuptools>=41.0.1',
                        'Markdown>=3.0.1',
                        'beautifulsoup4>=4.6.3',
                        'lxml>=4.2.5',
                        'pytz>=2019.1',
                        'pyjwt>=1.7.1',
]

EXTRAS_REQUIRE      = {
                        'dev':  [
                            'twine',
                            'pip',
                            'wheel',
                            'black',
                        ],
                        'widgets':[
                            'ipywidgets'
                        ],
                        'sso': [
                            'cryptography>=2.7',
                            'password-strength>=0.0.3',
                        ]
}

TEST_REQUIRE        = [
                        'pytest',
                        'pytest-pep8',
                        'pytest-docstyle',
                        'pytest-flakes',
                        'pytest-cov',

                        'PyQt',
                        'PyQt5'
]

PROJECT_URLS        = {
                        'Documentation': 'https://github.com/microsoft/jupyter-Kqlmagic/blob/master/README.md',
                        'Source': 'https://github.com/microsoft/jupyter-Kqlmagic',
}

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

LONG_DESCRIPTION = (README + '\n\n' + NEWS).replace('\r','')
LONG_DESCRIPTION_CONTENT_TYPE = 'text/x-rst'


class PyTest(TestCommand):

    user_options = [('pytest-args=', 'a', "Arguments to pass into py.test")]


    def initialize_options(self):
        TestCommand.initialize_options(self)
        try:
            from multiprocessing import cpu_count
            self.pytest_args = ['-n', str(cpu_count()), '--boxed']
        except (ImportError, NotImplementedError):
            self.pytest_args = ['-n', '1', '--boxed']


    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True


    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type=LONG_DESCRIPTION_CONTENT_TYPE,
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
        'Programming Language :: Python :: 3.6',
    ],
    keywords=KEYWORDS,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    maintainer=MAINTAINER,
    maintainer_email=MAINTAINER_EMAIL,
    url=URL,
    license=LICENSE,
    python_requires='>=3.6',
    packages=find_packages('azure'),
    package_dir={'': 'azure'},
    include_package_data=True,
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    project_urls=PROJECT_URLS,
    # cmdclass={'test': PyTest},
    # tests_require=TEST_REQUIRE,
)
