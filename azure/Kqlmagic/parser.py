# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import itertools
import os
import re
import json
import configparser as CP


import six
from traitlets import Bool, Int, Unicode, Enum, Float, TraitError


from .log import Logger, logger
from .my_utils import split_lex, adjust_path


class Parser(object):

    @classmethod
    def parse(cls, cell: str, config: dict, engines: list, user_ns: dict) -> list:
        """Separate input into (connection info, KQL statements, options)"""

        cell = cell.strip()
        parsed_queries = []
        cell, command = cls._parse_kql_command(cell, user_ns)
        if len(command) > 0 and command.get("command") != "submit":
            cell, options = cls._parse_kql_options(cell.strip(), config, user_ns)
            if cell: 
                raise ValueError(f"command {command.get('command')} has too many parameters")
            parsed_queries.append({"connection": "", "query": "", "options": options, "command": command})
            return parsed_queries

        # split to max 2 parts. First part, parts[0], is the first string.
        # parts = [part.strip() for part in cell.split(None, 1)]
        parts = split_lex(cell)
        
        # print(parts)
        if not parts:
            kql, options = cls._parse_kql_options("", config, user_ns)
            parsed_queries.append({"connection": "", "query": kql, "options": options, "command": {}})
            return parsed_queries

        #
        # replace substring of the form $name or ${name}, in windows also %name% if found in env variabes
        #

        connection = None

        conn_str = parts[0].strip()
        if not conn_str.startswith('-') and not conn_str.startswith('+'):
            if conn_str.startswith('"') and conn_str.endswith('"') or conn_str.startswith("'") and conn_str.endswith("'"):
                conn_str = conn_str[1:-1]

            #
            # connection taken from a section in  dsn file (file name have to be define in config.dsn_filename or specified as a parameter)
            #
            if conn_str.startswith("[") and conn_str.endswith("]"):
                section = conn_str[1:-1].strip()

                # parse to get flag, for the case that the file nema is specified in the options
                code = cell[len(parts[0]) :]
                kql, options = cls._parse_kql_options(code, config, user_ns)

                parser = CP.ConfigParser()
                dsn_filename = adjust_path(options.get("dsn_filename", config.dsn_filename))
                parser.read(dsn_filename)
                cfg_dict = dict(parser.items(section))

                cfg_dict_lower = {k.lower().replace("_", "").replace("-", ""): v for (k, v) in cfg_dict.items()}
                for e in engines:
                    if e._MANDATORY_KEY in cfg_dict_lower.keys():
                        all_keys = set(itertools.chain(*e._VALID_KEYS_COMBINATIONS))
                        connection_kv = [f"{k}='{v}'" for k, v in cfg_dict_lower.items() if v and k in all_keys]
                        connection = f"{e._URI_SCHEMA_NAME}://{';'.join(connection_kv)}"
                        break

            #
            # connection specified starting with one of the supported prefixes
            #
            elif "://" in conn_str:
                sub_parts = conn_str.strip().split("://", 1)
                if (len(sub_parts) == 2 and sub_parts[0].lower().replace("_", "").replace("-", "") in list(itertools.chain(*[e._ALT_URI_SCHEMA_NAMES for e in engines]))):
                    connection = conn_str
            #
            # connection specified as database@cluster
            #
            elif "@" in conn_str and "|" not in conn_str and "'" not in conn_str and '"' not in conn_str:
                connection = conn_str
        #
        # connection not specified, override default
        #
        if connection is None:
            connection = ""
            code = cell
        else:
            code = cell[len(parts[0]) :]

        #
        # split string to queries
        #
        queries = []
        queryLines = []
        for line in code.splitlines(True):
            if line.isspace():
                if len(queryLines) > 0:
                    queries.append("".join(queryLines))
                    queryLines = []
            else:
                queryLines.append(line)

        if len(queryLines) > 0:
            queries.append("".join(queryLines))

        suppress_results = False
        if len(queries) > 0 and queries[-1].strip() == ";":
            suppress_results = True
            queries = queries[:-1]

        if len(queries) == 0:
            queries.append("")

        #
        # parse code to kql and options
        #
        for query in queries:
            kql, options = cls._parse_kql_options(query.strip(), config, user_ns)
            kql = options.pop("query", None) or kql
            conn = options.pop("conn", None) or connection
            if suppress_results:
                options["suppress_results"] = True
            parsed_queries.append({"connection": conn.strip(), "query": kql, "options": options, "command": {}})

        return parsed_queries


    _COMMANDS_TABLE = {
        "version" : {"flag": "version", "type": "bool", "init": "False"},
        "banner" : {"flag": "banner", "type": "bool", "init": "False"},
        "usage" : {"flag": "usage", "type": "bool", "init": "False"},
        "submit" : {"flag": "submit", "type": "bool", "init": "False"}, # default
        "help" : {"flag": "help", "type": "str", "init": "None", "default": "help"},
        "faq": {"flag": "faq", "type": "bool", "init": "False"},
        "palette": {"flag": "palette", "type": "bool", "init": "False"},
        "palettes": {"flag": "palettes", "type": "bool", "init": "False"},
        "config": {"flag": "config", "type": "str", "init": "None", "default": "None"},
        # should be per connection
        "cache": {"flag": "cache", "type": "str", "init": "None"},
        "usecache": {"flag": "use_cache", "type": "str", "init": "None"},
        "schema": {"flag": "schema", "type": "str", "init": "None", "default": "None"},
        "clearssodb": {"flag": "clear_sso_db", "type": "bool", "init": "None", "default": "None"},
    }


    @classmethod
    def _parse_kql_command(cls, code: str, user_ns: dict) -> (str, dict):
        if not code.strip().startswith("--"):
            return (code.strip(), {})
        words = code.split()
        word = words[0][2:]
        if word.startswith("-"):
            raise ValueError(f"unknown command {words[0]}, commands' prefix is a double hyphen-minus, not a triple hyphen-minus")
        lookup_key = word.lower().replace("_", "").replace("-", "")
        obj = cls._COMMANDS_TABLE.get(lookup_key)
        if obj is None:
            raise ValueError(f"unknown command {words[0]}")

        trimmed_code = code
        trimmed_code = trimmed_code[trimmed_code.find(words[0]) + len(words[0]) :]

        _type = obj.get("type")
        if _type == "bool":
            param = True 
        elif len(words) >= 2 and not words[1].startswith("-"):
            param = cls.parse_value("command", obj, words[0], words[1], user_ns)
            trimmed_code = trimmed_code[trimmed_code.find(words[1]) + len(words[1]) :]
        elif obj.get("default") is not None:
            param = obj.get("default")
        else:
            raise ValueError(f"command {word[0]} is missing parameter")

        return (trimmed_code.strip(), {"command": obj.get("flag"), "param": param})


    _QUERY_PROPERTIES_TABLE = {
        # (OptionBlockSplittingEnabled): Enables splitting of sequence blocks after aggregation operator. [Boolean]
        "block_splitting_enabled": {"type": "bool"},

        # (OptionDatabasePattern): Database pattern overrides database name and picks the 1st database that matches the pattern. '*' means any database that user has access to. [String]
        "database_pattern": {"type": "str"},

        # (OptionDeferPartialQueryFailures): If true, disables reporting partial query failures as part of the result set. [Boolean]
        "deferpartialqueryfailures": {"type": "bool"},

        # (OptionMaxMemoryConsumptionPerQueryPerNode): Overrides the default maximum amount of memory a whole query may allocate per node. [UInt64]
        "max_memory_consumption_per_query_per_node": {"type": "uint"},

        # (OptionMaxMemoryConsumptionPerIterator): Overrides the default maximum amount of memory a query operator may allocate. [UInt64]
        "maxmemoryconsumptionperiterator": {"type": "uint"},

        # (OptionMaxOutputColumns): Overrides the default maximum number of columns a query is allowed to produce. [Long]
        "maxoutputcolumns": {"type": "uint"},

        # (OptionNoRequestTimeout): Enables setting the request timeout to its maximum value. [Boolean]
        "norequesttimeout": {"type": "bool"},

        # (OptionNoTruncation): Enables suppressing truncation of the query results returned to the caller. [Boolean]
        "notruncation": {"type": "bool"},

        # (OptionPushSelectionThroughAggregation): If true, push simple selection through aggregation [Boolean]
        "push_selection_through_aggregation": {"type": "bool"},

        # (OptionAdminSuperSlackerMode): If true, delegate execution of the query to another node [Boolean]
        "query_admin_super_slacker_mode": {"type": "bool"},

        #  (QueryBinAutoAt): When evaluating the bin_auto() function, the start value to use. [LiteralExpression]
        "query_bin_auto_at": {"type": "str"},

        # (QueryBinAutoSize): When evaluating the bin_auto() function, the bin size value to use. [LiteralExpression]
        "query_bin_auto_size": {"type": "str"},

        #  (OptionQueryCursorAfterDefault): The default parameter value of the cursor_after() function when called without parameters. [string]
        "query_cursor_after_default": {"type": "str"},

        #  (OptionQueryCursorAllowReferencingStreamingIngestionTables): Enable usage of cursor functions over databases which have streaming ingestion enabled. [boolean]
        "query_cursor_allow_referencing_streaming_ingestion_tables": {"type": "bool"},

        #  (OptionQueryCursorBeforeOrAtDefault): The default parameter value of the cursor_before_or_at() function when called without parameters. [string]
        "query_cursor_before_or_at_default": {"type": "str"},

        # (OptionQueryCursorCurrent): Overrides the cursor value returned by the cursor_current() or current_cursor() functions. [string]
        "query_cursor_current": {"type": "str"},

        #  (OptionQueryCursorScopedTables): List of table names that should be scoped to cursor_after_default .. cursor_before_or_at_default (upper bound is optional). [dynamic]
        "query_cursor_scoped_tables": {"type": "dict"},

        #  (OptionQueryDataScope): Controls the query's datascope -- whether the query applies to all data or just part of it. ['default', 'all', or 'hotcache']
        "query_datascope": {"type": "enum", "values": ['default', 'all', 'hotcache']},

        #  (OptionQueryDateTimeScopeColumn): Controls the column name for the query's datetime scope (query_datetimescope_to / query_datetimescope_from). [String]
        "query_datetimescope_column": {"type": "str"},

        #  (OptionQueryDateTimeScopeFrom): Controls the query's datetime scope (earliest) -- used as auto-applied filter on query_datetimescope_column only (if defined). [DateTime]
        "query_datetimescope_from": {"type": "str"},

        # (OptionQueryDateTimeScopeTo): Controls the query's datetime scope (latest) -- used as auto-applied filter on query_datetimescope_column only (if defined). [DateTime]
        "query_datetimescope_to": {"type": "str"},

        #  (OptionQueryDistributionNodesSpanSize): If set, controls the way sub-query merge behaves: the executing node will introduce an additional level in the query hierarchy for each sub-group of nodes; the size of the sub-group is set by this option. [Int]
        "query_distribution_nodes_span": {"type": "int"},

        #  (OptionQueryFanoutNodesPercent): The percentage of nodes to fanour execution to. [Int]
        "query_fanout_nodes_percent": {"type": "uint"},

        #  (OptionQueryFanoutThreadsPercent): The percentage of threads to fanout execution to. [Int]
        "query_fanout_threads_percent": {"type": "uint"},

        #  (OptionQueryLanguage): Controls how the query text is to be interpreted. ['csl','kql' or 'sql']
        "query_language": {"type": "enum", "values": ['csl', 'kql', 'sql']},

        #  (RemoteMaterializeOperatorInCrossCluster): Enables remoting materialize operator in cross cluster query.
        "query_materialize_remote_subquery": {"type": "bool"},

        # (OptionMaxEntitiesToUnion): Overrides the default maximum number of columns a query is allowed to produce. [Long]
        "query_max_entities_in_union": {"type": "uint"},

        # (OptionQueryNow): Overrides the datetime value returned by the now(0s) function. [DateTime]
        # note: cannot be relative to now()
        "query_now": {"type": "str"},

        #  (CostBasedOptimizerBroadcastJoinBuildMax): Max Rows count for build in broadcast join.
        "query_optimization_broadcast_build_maxSize": {"type": "uint"},

        #  (CostBasedOptimizerBroadcastJoinProbeMin): Min Rows count for probe in broadcast join.
        "query_optimization_broadcast_probe_minSize": {"type": "uint"},

        #  (CostBasedOptimizer): Enables automatic optimizations.
        "query_optimization_costbased_enabled": {"type": "bool"},

        #  (OptionOptimizeInOperator): Optimizes in operands serialization.
        "query_optimization_in_operator": {"type": "bool"},

        #  (CostBasedOptimizerShufflingCardinalityThreshold): Shuffling Cardinality Threshold.
        "query_optimization_shuffling_cardinality": {"type": "uint"},

        #  (OptionQueryRemoteEntitiesDisabled): If set, queries cannot access remote databases / clusters. [Boolean]
        "query_remote_entities_disabled": {"type": "bool"},

        #  (RemoteInOperandsInQuery): Enables remoting in operands.
        "query_remote_in_operands": {"type": "bool"},

        #  (OptionProgressiveQueryMinRowCountPerUpdate): Hint for Kusto as to how many records to send in each update (Takes effect only if OptionProgressiveQueryIsProgressive is set)
        "query_results_progressive_row_count": {"type": "uint"},

        #  (OptionProgressiveProgressReportPeriod): Hint for Kusto as to how often to send progress frames (Takes effect only if OptionProgressiveQueryIsProgressive is set)
        "query_results_progressive_update_period": {"type": "uint"},

        #  (OptionTakeMaxRecords): Enables limiting query results to this number of records. [Long]
        "query_take_max_records": {"type": "uint"},

        #  (OptionQueryConsistency): Controls query consistency. ['strongconsistency' or 'normalconsistency' or 'weakconsistency']
        "queryconsistency": {"type": "enum", "values": ['strongconsistency', 'normalconsistency', 'weakconsistency']},

        #  (OptionRequestCalloutDisabled): If set, callouts to external services are blocked. [Boolean]
        "request_callout_disabled": {"type": "bool"},

        #  (OptionRequestReadOnly): If specified, indicates that the request must not be able to write anything. [Boolean]
        "request_readonly": {"type": "bool"},

        #  (OptionResponseDynamicSerialization): Controls the serialization of 'dynamic' values in result sets. ['string', 'json']
        "response_dynamic_serialization": {"type": "enum", "values": ['string', 'json']},

        #  (OptionResponseDynamicSerialization_2): Controls the serialization of 'dynamic' string and null values in result sets. ['legacy', 'current']
        "response_dynamic_serialization_2": {"type": "enum", "values": ['legacy', 'current']},

        #  (OptionResultsProgressiveEnabled): If set, enables the progressive query stream
        "results_progressive_enabled": {"type": "bool"},

        #  (OptionSandboxedExecutionDisabled): If set, using sandboxes as part of query execution is disabled. [Boolean]
        "sandboxed_execution_disabled": {"type": "bool"},

        #  (OptionServerTimeout): Overrides the default request timeout. [TimeSpan]
        # is capped by 1hour
        "servertimeout": {"type": "str"},

        #  (OptionTruncationMaxRecords): Overrides the default maximum number of records a query is allowed to return to the caller (truncation). [Long]
        "truncationmaxrecords": {"type": "uint"},

        #  (OptionTruncationMaxSize): Overrides the dfefault maximum data size a query is allowed to return to the caller (truncation). [Long]
        "truncationmaxsize": {"type": "uint"},

        #  (OptionValidatePermissions): Validates user's permissions to perform the query and doesn't run the query itself. [Boolean]
        "validate_permissions": {"type": "bool"},
    }


    # all lookup keys in table, must be without spaces, underscores and hypthen-minus, because parser ignores them
    _OPTIONS_TABLE = {
        "ad": {"abbreviation": "autodataframe"},
        "autodataframe": {"flag": "auto_dataframe", "type": "bool", "config": "config.auto_dataframe"},
        "se": {"abbreviation": "shorterrors"},
        "shorterrors": {"flag": "short_errors", "type": "bool", "config": "config.short_errors"},
        "f": {"abbreviation": "feedback"},
        "feedback": {"flag": "feedback", "type": "bool", "config": "config.feedback"},
        "sci": {"abbreviation": "showconninfo"},
        "showconninfo": {"flag": "show_conn_info", "type": "str", "config": "config.show_conn_info"},
        "c2lv": {"abbreviation": "columnstolocalvars"},
        "columnstolocalvars": {"flag": "columns_to_local_vars", "type": "bool", "config": "config.columns_to_local_vars"},
        "sqt": {"abbreviation": "showquerytime"},
        "showquerytime": {"flag": "show_query_time", "type": "bool", "config": "config.show_query_time"},
        "sq": {"abbreviation": "showquery"},
        "showquery": {"flag": "show_query", "type": "bool", "config": "config.show_query"},
        "sql": {"abbreviation": "showquerylink"},
        "showquerylink": {"flag": "show_query_link", "type": "bool", "config": "config.show_query_link"},
        "qld": {"abbreviation": "querylinkdestination"},
        "querylinkdestination": {"flag": "query_link_destination", "type": "str", "config": "config.query_link_destination"},

        "esr": {"abbreviation": "enablesuppressresult"},
        "enablesuppressresult": {"flag": "enable_suppress_result", "type": "bool", "config": "config.enable_suppress_result"},
        "pfi": {"abbreviation": "plotlyfsincludejs"},
        "plotlyfsincludejs": {"flag": "plotly_fs_includejs", "type": "bool", "config": "config.plotly_fs_includejs"},
        "pw": {"abbreviation": "popupwindow"},
        "popupwindow": {"flag": "popup_window", "type": "bool", "init": "False"},
        "al": {"abbreviation": "autolimit"},
        "autolimit": {"flag": "auto_limit", "type": "int", "config": "config.auto_limit"},
        "dl": {"abbreviation": "displaylimit"},
        "displaylimit": {"flag": "display_limit", "type": "int", "config": "config.display_limit"},
        "wait": {"abbreviation": "timeout"},
        "to": {"abbreviation": "timeout"},
        "timeout": {"flag": "timeout", "type": "int", "config": "config.timeout"},
        "ptst": {"abbreviation": "prettytablestyle"},
        "prettytablestyle": {"flag": "prettytable_style", "type": "str", "config": "config.prettytable_style"},
        "var": {"abbreviation": "lastrawresultvar"},
        "lastrawresultvar": {"flag": "last_raw_result_var", "type": "str", "config": "config.last_raw_result_var"},
        "tp": {"abbreviation": "tablepackage"},
        "tablepackage": {"flag": "table_package", "type": "str", "config": "config.table_package"},
        "pp": {"abbreviation": "plotpackage"},
        "plotpackage": {"flag": "plot_package", "type": "str", "config": "config.plot_package"},
        "df": {"abbreviation": "dsnfilename"},
        "dsnfilename": {"flag": "dsn_filename", "type": "str", "config": "config.dsn_filename"},
        "vc": {"abbreviation": "validateconnectionstring"},
        "validateconnectionstring": {"flag": "validate_connection_string", "type": "bool", "config": "config.validate_connection_string"},
        "aps": {"abbreviation": "autopopupschema"},
        "autopopupschema": {"flag": "auto_popup_schema", "type": "bool", "config": "config.auto_popup_schema"},
        "jd": {"abbreviation": "jsondisplay"},
        "jsondisplay": {"flag": "json_display", "type": "str", "config": "config.json_display"},
        "sjd": {"abbreviation": "schemajsondisplay"},
        "schemajsondisplay": {"flag": "schema_json_display", "type": "str", "config": "config.schema_json_display"},
        "pd": {"abbreviation": "palettedesaturation"},
        "palettedesaturation": {"flag": "palette_desaturation", "type": "float", "config": "config.palette_desaturation"},
        "pn": {"abbreviation": "palettename"},
        "paramsdict": {"flag": "params_dict", "type": "dict", "init": "None"},
        "palettename": {"flag": "palette_name", "type": "str", "config": "config.palette_name"},
        "cache": {"flag": "cache", "readonly": "True", "type": "str", "config": "config.cache"},
        "usecache": {"flag": "use_cache", "readonly": "True", "type": "str", "config": "config.use_cache"},
        
        "tempfoldername": {"flag": "temp_folder_name", "readonly": "True", "type": "str", "config": "config.temp_folder_name"},
        "cachefoldername": {"flag": "cache_folder_name", "readonly": "True", "type": "str", "config": "config.cache_folder_name"},
        "exportfoldername": {"flag": "export_folder_name", "readonly": "True", "type": "str", "config": "config.export_folder_name"},
        "addkqlreftohelp": {"flag": "add_kql_ref_to_help", "readonly": "True", "type": "bool", "config": "config.add_kql_ref_to_help"},
        "addschematohelp": {"flag": "add_schema_to_help", "readonly": "True", "type": "bool", "config": "config.add_schema_to_help"},
        "notebookapp": {"flag": "notebook_app", "readonly": "True", "type": "str", "config": "config.notebook_app"},

        "checkmagicversion": {"flag": "check_magic_version", "readonly": "True", "type": "bool", "config": "config.check_magic_version"},
        "showwhatnew": {"flag": "show_what_new", "readonly": "True", "type": "bool", "config": "config.show_what_new"},
        "showinitbanner": {"flag": "show_init_banner", "readonly": "True", "type": "bool", "config": "config.show_init_banner"},
        
        "testnotebookapp": {"flag": "test_notebook_app", "readonly": "True", "type": "str", "config": "config.test_notebook_app"},

        "cloud": {"flag": "cloud", "type": "str", "config": "config.cloud"},
        "enablesso": {"flag": "enable_sso", "type": "bool", "config": "config.enable_sso"},
        "ssodbgcinterval": {"flag": "sso_db_gc_interval", "type": "int", "config": "config.sso_db_gc_interval"},

        "tryazclilogin": {"flag": "try_azcli_login", "type": "bool", "config": "config.try_azcli_login"},
        "tryazcliloginsubscription": {"flag": "try_azcli_login_subscription", "type": "str", "config": "config.try_azcli_login_subscription"},

        "idtag": {"abbreviation": "requestidtag"},
        "requestidtag": {"flag": "request_id_tag", "type": "str", "config": "config.request_id_tag"},

        "apptag": {"abbreviation": "requestapptag"},
        "requestapptag": {"flag": "request_app_tag", "type": "str", "config": "config.request_app_tag"},

        "usertag": {"abbreviation": "requestusertag"},
        "requestusertag": {"flag": "request_user_tag", "type": "str", "config": "config.request_user_tag"},

        "dcln": {"abbreviation": "devicecodeloginnotification"},
        "devicecodeloginnotification": {"flag": "device_code_login_notification", "type": "str", "config": "config.device_code_login_notification"},

        "dcne": {"abbreviation": "devicecodenotificationemail"},
        "devicecodenotificationemail": {"flag": "device_code_notification_email", "type": "str", "config": "config.device_code_notification_email"},

        "saveas": {"flag": "save_as", "type": "str", "init": "None"},
        "saveto": {"flag": "save_to", "type": "str", "init": "None"},
        "query": {"flag": "query", "type": "str", "init": "None"},
        "conn": {"flag": "conn", "type": "str", "init": "None"},
        "queryproperties": {"flag": "query_properties", "type": "dict", "init": "None"},

        "pc": {"abbreviation": "palettecolors"},
        "palettecolors": {"flag": "palette_colors", "type": "int", "config": "config.palette_colors"},
        "pr": {"abbreviation": "palettereverse"},
        "palettereverse": {"flag": "palette_reverse", "type": "bool", "init": "False"},

        "ps": {"abbreviation": "popupschema"},
        "popupschema": {"flag": "popup_schema", "type": "bool", "init": "False"},

        "did": {"abbreviation": "displayid"},
        "displayid": {"flag": "display_id", "type": "bool", "init": "False"},
        "displayhandlers": {"flag": "display_handlers", "type": "dict", "init": "{}"},

        "pi": {"abbreviation": "popupinteraction"},        
        "popupinteraction": {"flag": "popup_interaction", "type": "str", "config": "config.popup_interaction"},
        "tempfilesserver": {"flag": "temp_files_server", "readonly": "True", "type": "str", "config": "config.temp_files_server"},
        "tempfilesserveraddress": {"flag": "temp_files_server_address", "readonly": "True", "type": "str", "config": "config.temp_files_server_address"},
        
        "kernellocation": {"flag": "kernel_location", "readonly": "True", "type": "str", "config": "config.kernel_location"},
        "kernelid": {"flag": "kernel_id", "readonly": "True", "type": "str", "config": "config.kernel_id"},

        "notebookserviceaddress": {"flag": "notebook_service_address", "readonly": "True", "type": "str", "config": "config.notebook_service_address"},

        "dtd": {"abbreviation": "dynamictodataframe"},
        "dynamictodataframe": {"flag": "dynamic_to_dataframe", "type": "str", "config": "config.dynamic_to_dataframe"},

        "tempfolderlocation": {"flag": "temp_folder_location", "readonly": "True", "type": "str", "config": "config.temp_folder_location"},

        "pl": {"abbreviation": "plotlylayout"},
        "plotlylayout": {"flag": "plotly_layout", "type": "dict", "config": "config.plotly_layout"},
        
    }

    reserved_list = [
        "format", "scale", "width", "height", "filename","show"
        "encoding",
        "button_text"
    ]


    @classmethod
    def validate_override(cls, name, config:dict, **override_options):
        """validate the provided option are valid"""

        options = {}
        for key, value in override_options.items():
            lookup_key = key.lower().replace("-", "").replace("_", "")
            obj = cls._OPTIONS_TABLE.get(lookup_key)
            if obj is not None:
                if obj.get("abbreviation"):
                    obj = cls._OPTIONS_TABLE.get(obj.get("abbreviation"))
                if obj.get("readonly"):
                    raise ValueError(f"option '{key}' in {name} is readony, cannot be set")
                cls._convert(name, obj, key, value)
                cls._validate_config_trait(name, obj, key, value, config)
                options[obj.get("flag") or lookup_key] = value
            else:
                raise ValueError(f"unknown option '{key}' in {name}")
        return options


    @classmethod
    def parse_option(cls, name:str, key:str, val:str, config:dict=None, lookup:dict=None):
        """validate the provided option are valid
           return normalized key and value"""
        lookup_key = key.lower().replace("-", "").replace("_", "")
        lookup_table = lookup or cls._OPTIONS_TABLE
        obj = lookup_table.get(lookup_key)
        if obj is not None:
            if obj.get("abbreviation"):
                obj = lookup_table.get(obj.get("abbreviation"))
            if obj.get("type") == "str" and val == "":
                value = ""
            else:
                try:
                    eval_value = eval(val)
                except NameError as e:
                    if obj.get("type") == "str":
                        eval_value = val
                    else:
                        raise e
                value = cls._convert(name, obj, key, eval_value)
            if config is not None:
                cls._validate_config_trait(name, obj, key, value, config)
            return obj.get("flag", key), value

    @classmethod
    def parse_option_key(cls, name:str, key:str, config:dict):
        """validate the provided option key is valid
           return normalized key"""
        lookup_key = key.lower().replace("-", "").replace("_", "")
        obj = cls._OPTIONS_TABLE.get(lookup_key)
        if obj is not None:
            if obj.get("abbreviation"):
                obj = cls._OPTIONS_TABLE.get(obj.get("abbreviation"))
            if obj.get("config") is not None:
                return obj.get("flag"), getattr(config, obj.get("flag"))


    @classmethod
    def _parse_kql_options(cls, code: str, config: dict, user_ns: dict) -> (str, dict):
        words = code.split()
        options = {}
        properties = {}
        table = options

        for value in cls._OPTIONS_TABLE.values():
            if value.get("config"):
                options[value.get("flag")] = eval(value.get("config"))
            elif value.get("init"):
                options[value.get("flag")] = eval(value.get("init"))

        if not words:
            return ("", options)
        num_words = len(words)
        trimmed_kql = code
        first_word = 0

        if num_words - first_word >= 2 and words[first_word + 1] == "<<":
            options["result_var"] = words[first_word]
            trimmed_kql = trimmed_kql[trimmed_kql.find("<<") + 2 :]
            first_word += 2

        key_state = True
        is_option = True
        is_property = False
        for word in words[first_word:]:
            if key_state:
                is_option = word[0].startswith("-")
                is_property = word[0].startswith("+")
                if not is_option and not is_property:
                    break
                # validate it is not a command
                if is_option and word[0].startswith("--"):
                    raise ValueError(f"invalid option {word[0]}, cannot start with a bouble hyphen-minus")

                trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word) :]
                word = word[1:]
                bool_value = True
                if word[0].startswith("!"):
                    bool_value = False
                    word = word[1:]
                if "=" in word:
                    parts = word.split("=", 1)
                    key = parts[0]
                    value = parts[1]
                else:
                    key = word
                    value = None

                if is_option:
                    lookup_key = key.lower().replace("-", "").replace("_", "")
                    obj = cls._OPTIONS_TABLE.get(lookup_key)
                    table = options 
                else:
                    lookup_key = key.lower()
                    obj = cls._QUERY_PROPERTIES_TABLE.get(lookup_key)
                    table = properties 

                if obj is not None:
                    if obj.get("abbreviation"):
                        obj = cls._OPTIONS_TABLE.get(obj.get("abbreviation"))
                    if obj.get("readonly"):
                        raise ValueError(f"option {key} is readony, cannot be set")

                    _type = obj.get("type")
                    opt_key = obj.get("flag") or lookup_key
                    if _type == "bool" and value is None:
                        table[opt_key] = bool_value
                    else:
                        if not bool_value:
                            raise ValueError(f"option {key} cannot be negated")
                        if value is not None:
                            table[opt_key] = cls.parse_value("options", obj, key, value, user_ns)
                        else:
                            key_state = False
                else:
                    raise ValueError("unknown option")
            else:
                trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word) :]
                table[opt_key] = cls.parse_value("options", obj, key, word, user_ns)
                key_state = True
            first_word += 1

            # validate using config traits
            if key_state:
                cls._validate_config_trait("options", obj, key, options[opt_key], config)
            
        if not key_state:
            raise ValueError("last option is missing parameter")

        if (options["query_properties"]):
            properties.update(options["query_properties"])
        options["query_properties"] = properties 
        if num_words - first_word > 0:
            last_word = words[-1].strip()
            if last_word.endswith(";"):
                options["suppress_results"] = True
                trimmed_kql = trimmed_kql[: trimmed_kql.rfind(";")]
        return (trimmed_kql.strip(), options)


    @classmethod
    def parse_and_get_kv_string(cls, conn_str: str, user_ns: dict) -> dict:

        matched_kv = {}
        rest = conn_str
        delimiter_required = False
        lp_idx = rest.find("(") 
        eq_idx = rest.find("=") 
        sc_idx = rest.find(";") 
        l_char = "(" if eq_idx < 0 and sc_idx < 0 else "=" if lp_idx < 0 else "(" if lp_idx < eq_idx and lp_idx < sc_idx else "="
        r_char = ")" if l_char == "(" else ";"
        extra_delimiter = None if r_char == ";" else "."

        while len(rest) > 0:
            l_idx = rest.find(l_char)
            r_idx = rest.find(r_char)
            if l_idx < 0:
                if l_char == "(":
                    # string ends with delimiter
                    if extra_delimiter is not None and extra_delimiter == rest:
                        break
                    else:
                        raise ValueError("invalid key/value string, missing left parethesis.")
                # key only at end of string
                elif r_idx < 0:
                    key = rest
                    val = ""
                    rest = ""
                # key only
                else:
                    key = rest[:r_idx].strip()
                    val = ""
                    rest = rest[r_idx + 1 :].strip()
            # key only
            elif r_idx >= 0 and r_idx < l_idx:
                if l_char == "(":
                    raise ValueError("invalid key/value string, missing left parethesis.")
                else:
                    key = rest[:r_idx].strip()
                    val = ""
                    rest = rest[r_idx + 1 :].strip()
            # key and value
            else:
                key = rest[:l_idx].strip()
                rest = rest[l_idx + 1 :].strip()
                r_idx = rest.find(r_char)
                if r_idx < 0:
                    if l_char == "(":
                        raise ValueError("invalid key/value string, missing right parethesis.")
                    else:
                        val = rest
                        rest = ""
                else:
                    val = rest[:r_idx].strip()
                    rest = rest[r_idx + 1 :].strip()
                if extra_delimiter is not None:
                    if key.startswith(extra_delimiter):
                        key = key[1:].strip()
                    elif delimiter_required:
                        raise ValueError("invalid key/value string, missing delimiter.")
                    delimiter_required = True

            # key exist
            if len(key) > 0:
                val = cls.parse_value("key/value", {"type": "str"}, key, val, user_ns)
                lookup_key = key.lower().replace("-", "").replace("_", "")
                matched_kv[lookup_key] = val
            # no key but value exist
            elif len(val) > 0:
                raise ValueError("invalid key/value string, missing key.")
            # no key, no value in parenthesis mode
            elif l_char == "(":
                raise ValueError("invalid key/value string, missing key.")

        return matched_kv


    @classmethod
    def parse_value(cls, name:str, obj, key:str, value:str, user_ns: dict):

        _type = obj.get("type")

        if value == "" and _type == "str":
            return value

        if value.startswith('$'):
            val = os.getenv(value[1:])
        else:
            val = eval(value, None, user_ns)

        # check value is of the right type
        try:
            return cls._convert(name, obj, key, val)
        except:
            return cls._convert(name, obj, key, eval(val))



    @classmethod
    def _convert(cls, name, obj, key, value):
        try:
            _type = obj.get("type")
            if _type == "int":
                if float(value) != int(value):
                    raise ValueError
                return int(value)
            elif _type == "uint":
                if float(value) != int(value) or int(value) < 0:
                    raise ValueError
                return int(value)                 
            elif _type == "float":
                return float(value)
            elif _type == "bool":
                if type(value) == str:
                    if value == 'True':
                        return True
                    elif value == 'False':
                        return False
                    else:
                        raise ValueError
                elif bool(value) != int(value):
                    raise ValueError
                return bool(value)
            elif _type == "dict":
                return dict(value)
            elif _type == "enum":
                enum_values = obj.get("values", [])
                if enum_values.index(value) >= 0:
                    pass
                else:
                    raise ValueError
            else:
                return str(value)

        except:
            raise ValueError(f"failed to set option '{key}' in {name}, due to invalid '{_type}' of value '{value}'.")


    @classmethod
    def _validate_config_trait(cls, name, obj, key, value, config: dict):           
        # validate using config traits
        option_config = obj.get("config")
        if option_config is not None:
            #
            # save current value
            #
            saved_value = eval(option_config)

            #
            # prepare statement
            #
            _type = obj.get("type")
            if _type == "str":
                value_str = "'" + value.replace("'", "\\'") + "'" if value is not None else None
                saved_value_str = ("'" + saved_value.replace("'", "\\'") + "'") if saved_value is not None else None
            else:
                value_str = f"{value}"
                saved_value_str = f"{saved_value}"
            set_value_statement = f"{option_config} = {value_str}"
            set_saved_value_statement = f"{option_config} = {saved_value_str}"

            #
            # try to modify value
            #
            exception = None
            try:
                exec(set_value_statement)
            except:
                exception = ValueError(f"failed to set option '{key}' in {name}, due to invalid value '{value}'.")

            #
            # restore value
            #
            exec(set_saved_value_statement)
            if exception is not None: 
                raise exception
