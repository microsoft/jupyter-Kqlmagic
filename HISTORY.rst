fix missing lxml.py module dependency in setup.py; created history.rst file; updated setup.py based on azure notebooks image - 0.1.93 - Mar 24, 2019
support database names with whitespaces; support ADX proxy over AI/LA cluster names; fix charts based on firest quantity column - 0.1.92 - Mar 18, 2019
allow v1 response from kusto api v2, support adx-proxy, - 0.1.91 - Feb 20, 2019
fix to_dataframe bool - 0.1.90 - Jan 25, 2019
update binder requirements to 0.1.88 - Jan 16, 2019
version 01.88, added --schema command - 0.1.88 - Jan 16, 2019
added test_notebook_app for testing, , commands instead of show_window return html object result. - 0.1.87 - Dec 25, 2018
added run_upgrade.bat; enhanced run_tests.bat - Dec 24, 2018
fixed run_tests.bat - fixed run_tests.bat - Dec 20, 2018
update binder requirements - update binder requirements - Dec 20, 2018
Adjusted to support VisualStudioCode (Jupyter) notebook - 0.1.86 - Dec 18, 2018
added support to "ipython" notebook_app, started a tests suits - 0.1.85 - Dec 17, 2018
update binder requirements to 0.1.84 - Dec 16, 2018
Fix dynamic columns that are not strict json - 0.1.84 - Dec 11, 2018
Align to Kusto new V2 dynamic field response; fix kql.submit() - 0.1.83 - Dec 10, 2018
binder to use 0.1.82 - Dec 9, 2018
added binder folder, with requirements - Dec 9, 2018
fixed env KQLMAGIC_CONNECTION_STR - 0.1.82 - Dec 6, 2018
update notebooks with anonymous authentication - Dec 3, 2018
removed tell_format; added anonymous authentication (for the case of local data source) - 0.1.81 - Dec 3, 2018
Removed dependency on azure.kusto sdk, use rest api to kusto. - 0.1.80 - Nov 13, 2018
changed cache management via commands, modified caches to be named, added option -save_to folder,; support datetime as linear value in charts, support kql render attributes; support fully qualified cluster; prepare to remove kusto sdk - 0.1.79 - Nov 13, 2018
support query option -timout / -wait / -to in seconds - 0.1.78 - Nov 8, 2018
fix popup for clusters or databases with special characters, fix .ingest online, version - 0.1.77 - Nov 8, 2018
restrict Kqlmagic to python >= 3.6 - 0.1.76 - Nov 8, 2018
support command --help without params - 0.1.75 - Nov 7, 2018
version 0.1.74 - 0.1.74 - Nov 7, 2018
support .show database DB schema as json - Nov 7, 2018
fix parametrization of df column of type object but is actually bytes 0.1.73 - 0.1.73 - Nov 5, 2018
fixed parametrizaton to .set management queries, fixed javascript error when old out cell displays - 0.1.72 - Nov 5, 2018
prepare support all visualization properties - 0.1.71 - Nov 3, 2018
minor changes in readme and help - Oct 31, 2018
support pandas dataframe as parameter, add support null values in conversion to dataframes, fixed pretty print of dynamic cols; improved parametrization. - 0.1.70 - Oct 31, 2018
make keys as caseinsensitive, ignore underscore and hyphen-minus, covert some options to commands, modify kusto kql logo, and remove kusto name. 0.1.69 - 0.1.69 - Oct 29, 2018
fix clors notebook - Oct 25, 2018
popup_window option to all commands, fixed banner, update notebooks, 0.1.68 - 0.1.68 - Oct 25, 2018
update notebooks with --version,; allow =setting in options, more quotes flexibility with values, support option dict type; - 0.1.67 - Oct 25, 2018
support partial result, add command concept, added commands, 0.1.66 - 0.1.66 - Oct 25, 2018
options and connection key values can be parametyrize from python and env variables, new -query and -conn option - 0.1.65 - Oct 23, 2018
parametrized options, add file://folder, update color notebook, 0.1.64 - 0.1.64 - Oct 22, 2018
fix notebooks - Oct 22, 2018
run black on code, version 0.1.63 - 0.1.63 - Oct 22, 2018
bug fix, code refactor, version 0.1.62 - 0.1.62 - Oct 21, 2018
moved the saved_as earlier in the pipe, to capture raw results even if there is an error later. version 0.1.61 - 0.1.61 - Oct 20, 2018
fix to pandas convertion, for the case of missing int64 or missing bool, version 0.1.60 - 0.1.60 - Oct 18, 2018
update notebooks, added support to certificate pem_file, version 0.1.59 - 0.1.59 - Oct 18, 2018
restructure local files - Oct 18, 2018
Update notebooks, created QuikStart fro log analytics notebook, updated README, version 0.1.58 - 0.1.58 - Oct 18, 2018
update notebooks - Oct 18, 2018
update notebooks - Oct 18, 2018
update notebooks with new connection string format - Oct 18, 2018
Fixed setup.py long description to show properly work on PyPI - 0.1.57 -  Oct 17, 2018
removed setup dwonload_url - Oct 17, 2018
update setup description and README titles - Oct 17, 2018
update README - Oct 17, 2018
update README - Oct 17, 2018
update README - Oct 17, 2018
updated README, and setup - 0.1.56 - Oct 17, 2018
fix setup.py - fix setup.py
removed version restriction on traitlets - 0.1.55 - Oct 17, 2018
removed psutil version restriction in setup - 0.1.54 - Oct 17, 2018
fixed pallette notebook; adjuset to azure-kusto-data version 0.0.15 changes (tenant is a must parameter, for clienid) - 0.1.53 - Oct 16, 2018
Fixed development status, updated setup, version 0.1.52 - 0.1.52 - Oct 16, 2018
changes state from 1-alpha to 3-beta; published in PyPI; modified notebooks to reflect the changes; changed structure of files. Version 0.1.50 - 0.1.50 -  Oct 16, 2018
support alt schema names; simplify code; improve connection inheritance; ; added warning to upgrade version with PyPI; added alias to connection string; added alias_magics 'adxql'; - 0.1.47 - Oct 15, 2018
removed order restriction on connection string, simplified and unified connections string code parsing, improved error presentation, added friendly_ to connection string (for case id is a uuid) - 0.1.46 - Oct 10, 2018
AAD Authentication Code() and Clientid/Clientsecret, added to ApplicationInsights and Loganalytics - 0.1.45 - Oct 9, 2018
add aad auth support for lognalytics and applicationinsights; simplified code - 0.1.44 - Oct 8, 2018
support getting generic database schema from all engines - 0.1.43 - Oct 7, 2018
fixed fork charts, adjusted to work on Jupyter Lab - 0.1.42 - Oct 7, 2018
fixed linechart and timechart multidimetional and not sorted, and not ordered, 0.1.41 - 0.1.41 - Oct 4, 2018
bump version 0.1.40
bump 0.1.39
fix results.py -  Oct 3, 2018
bump version 0.1.38
patch to support plotly in Azure notebook, till it will support plotly MIME - Oct 3, 2018
comment local usage - Oct 3, 2018
add ParametrizeYourQuery notebook - Oct 3, 2018
added parametrization feature, added saved_as option, added params_dict option, fixed options validation, added parametrized_query attribute to results object, updated setup.py - 0.1.37 - Sep 30, 2018
simplified code, fixed cahing schema - 0.1.36 - Sep 24, 2018
Fixed timespan - 0.1.35 - Sep 23, 2018
fixed ref to github - Sep 20, 2018
Added copyright file header to all files, version 0.1.34 - 0.1.34 - Sep 20, 2018
fix notebooks - 0.1.33 - Sep 19, 2018
fix setup.py - 0.1.32 - Sep 19, 2018
bump version - 0.1.31 - Sep 19, 2018
