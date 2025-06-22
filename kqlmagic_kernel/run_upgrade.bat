@echo off
echo ====================================
echo =      Kqlmagic Kernel package     =
echo ====================================
REM
REM clean folders used by setuptools and wheel
echo ----- clean folders to be used by setuptools and wheel
rmdir /Q /S build
rmdir /Q /S dist
REM
echo ----- create sdist bdist_wheel
python setup.py sdist bdist_wheel
if errorlevel 1 (
    echo ----- Failed to to create wheel -----
    PAUSE
    exit /b 1
)
REM
echo ----- Kqlmagic Kernel REVIEW SETUP PARAMETERS
PAUSE
REM review what is included in dist
REM check dist is OK
echo ----- check create dist is OK
twine check dist/*
if errorlevel 1 (
    echo ----- invalid dist, twine check failed -----
    PAUSE
    exit /b 1
)
REM
if not defined PYPI_USERNAME (
    echo ----- environment variable PYPI_USERNAME is not defined  -----
    PAUSE
    exit /b 1
)
REM
if not defined PYPI_PASSWORD (
    echo ----- environment variable PYPI_PASSWORD is not defined  -----
    PAUSE
    exit /b 1
)
REM
REM upload package to PYPI
echo ----- upload dist to PYPI
twine upload -u %PYPI_USERNAME% -p %PYPI_PASSWORD% dist/*
if errorlevel 1 (
    echo ----- failed to uplad dist -----
    PAUSE
    exit /b 1
)
REM
REM cleanup
echo ----- cleanup folders used by setuptools and wheel
rmdir /Q /S build
rmdir /Q /S dist