REM clean folders used by setuptools and wheel
del /Q build
del /Q dist
REM prepare package fot PYPI
python setup.py sdist bdist_wheel
REM upload package to PYPI
twine upload -u %PYPI_USERNAME% -p %PYPI_PASSWORD% dist/*
REM cleanup
del /Q build
del /Q dist
REM reminder to upgrade the dependencies in binder/requirements.txt
PAUSE ----- UPDATE version in binder/requirements.txt -----
