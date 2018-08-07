"""Setup for Kqlmagic"""

DESCRIPTION         = "Kqlmagic: KQL (Kusto Query Language) execution via Jupyter magic"

NAME                = "Kqlmagic"

AUTHOR              = 'Michael Binshtock'

AUTHOR_EMAIL        = 'mbnshtck@gmail.com'

URL                 = 'https://pypi.python.org/pypi/jupyter-kql-magic'

DOWNLOAD_URL        = 'https://pypi.python.org/pypi/jupyter-kql-magic'

LICENSE             = 'MIT License'

KEYWORDS            = 'database ipython jupyter notebook kql kusto loganalytics applicationinsights'

INSTALL_REQUIRES    = [
                        'prettytable>=0.7.2',
                        'ipython>=1.0',
                        'azure-kusto-data>=0.0.2',
                        'six',
                        'pgspecial',
                        'ipython-genutils>=0.1.0',
]


# To use a consistent encoding
import codecs

import re

from os import path

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

CURRENT_PATH = path.abspath(path.dirname(__file__))
PACKAGE_PATH = 'src-kql'.replace('-', path.sep)

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
        'Development Status :: 1 - Alpha',
        'Intended Audience :: Science/Research',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2',
    ],
    keywords=KEYWORDS,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    download_url=DOWNLOAD_URL,
    license=LICENSE,
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    include_package_data=True,
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
)
