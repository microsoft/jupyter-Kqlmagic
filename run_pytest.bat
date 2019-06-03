REM !/bin/bash
REM
REM In case anaconada is not activated, uncomment next line
REM C:/Users/michabin/AppData/Local/Continuum/anaconda3/Scripts/activate.bat
REM
REM set environment variables that are needed by the tests
set TEST_CONNECTION_STR=appinsights://appid='DEMO_APP';appkey='DEMO_KEY'
set KQLMAGIC_NOTEBOOK_APP=ipython
set KQLMAGIC_CONFIGURATION=auto_popup_schema=False;test_notebook_app='jupyternotebook'
REM
REM change directory to the location of the tests
cd azure\tests
REM
REM Execute nose to run the tests in the context of jupyter / ipython
ipython -c "import pytest; pytest.main([])" --matplotlib='qt'


REM Insert breakpoints with `from nose.tools import set_trace; set_trace()`
REM
REM we are done
pause "tests finished !!!"
set TEST_CONNECTION_STR=
set KQLMAGIC_NOTEBOOK_APP=
set KQLMAGIC_CONFIGURATION=

cd ..\..
