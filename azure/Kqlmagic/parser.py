# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Tuple, Dict, List, Any
import itertools
import configparser as CP
from datetime import timedelta, datetime

import dateutil.parser
from traitlets import TraitType
from traitlets.config.configurable import Configurable


from ._debug_utils import debug_print 
from .dependencies import Dependencies
from .constants import Constants, Schema
from .my_utils import split_lex, adjust_path, is_env_var, get_env_var, is_collection, strip_if_quoted, get_lines
from .engine import Engine

from .commands_table import COMMANDS_TABLE


class Parser(object):

    default_options:Dict[str,Any] = {}
    traits_dict:Dict[str,TraitType] = {}

    @classmethod
    def initialize(cls, config:Configurable):
        cls.traits_dict = config.traits()
        config.observe(Parser.observe_config_changes)
        cls.init_default_options(config)

    @staticmethod
    def observe_config_changes(change:Dict[str,str]): 
        if change.get('type') == 'change':
            name = change.get('name')
            obj = Parser._OPTIONS_TABLE.get(name.lower().replace("-", "").replace("_", ""))
            if "init" not in obj:
                Parser.default_options[change.get('name')] = change.get('new')


    @classmethod
    def init_default_options(cls, config:Configurable):
        for obj in cls._OPTIONS_TABLE.values():
            if "abbreviation" not in obj:
                name = obj.get("flag")
                if "init" in obj:
                    cls.default_options[name] = obj.get("init")
                elif name in cls.traits_dict:
                    cls.default_options[name] = getattr(config, name)
                elif "abbreviation" not in obj:
                    raise Exception(f"missing init in _OPTIONS_TABLE for option: {name}")


    @classmethod
    def parse(cls, _line:str, _cell:str, config:Configurable, engines:List[Engine], user_ns:Dict[str,Any])->List[Dict[str,Any]]:
        is_cell = _cell is not None
        cell = f"{_line}\n{_cell or ''}"
        cell = cell.strip()
        code = cell
        #
        # split string to queries
        #
        suppress_all_results = False
        # tuple: 
        sections:List[Dict[str,str]] = []
        if is_cell:
            magic_section_name = Constants.MAGIC_NAME
            section_lines:List[str] = []
            previous_line = " "  # should be init to space for the below to work
            for line in get_lines(code):
                lstripped_line = line.lstrip()

                if (lstripped_line.startswith(Constants.IPYKERNEL_CELL_MAGIC_PREFIX)
                        and (previous_line.isspace() or (len(sections) > 0 and sections[-1].get("type") == "line_magic"))):
                    if len(section_lines) > 0:
                        sections.append({"type": "cell_magic", "name": magic_section_name, "body": "".join(section_lines)})

                    magic_word = lstripped_line.split(None, 1)[0]
                    magic_section_name = magic_word[len(Constants.IPYKERNEL_CELL_MAGIC_PREFIX):]
                    lstripped_line = lstripped_line[len(magic_word):].lstrip()
                    if magic_section_name == Constants.MAGIC_NAME:
                        section_lines = [] if lstripped_line.isspace() else [lstripped_line]
                    else:
                        section_lines = [lstripped_line]

                elif magic_section_name != Constants.MAGIC_NAME:
                    section_lines.append(line)

                elif (lstripped_line.startswith(Constants.IPYKERNEL_LINE_MAGIC_PREFIX)
                      and (previous_line.isspace() or (len(sections) > 0 and sections[-1].get("type") == "line_magic"))):
                    magic_word = lstripped_line.split(None, 1)[0]
                    magic_name = magic_word[len(Constants.IPYKERNEL_LINE_MAGIC_PREFIX):]
                    lstripped_line = lstripped_line[len(magic_word):].lstrip()
                    if magic_name == Constants.MAGIC_NAME:
                        if not lstripped_line.isspace():
                            sections.append({"type": "line_magic", "name": magic_name, "body": lstripped_line})
                    else:
                        sections.append({"type": "line_magic", "name": magic_name, "body": lstripped_line})

                elif line.isspace():
                    if len(section_lines) > 0:
                        not_commented_lines = [1 for seg_line in section_lines if not seg_line.lstrip().startswith("//")]
                        if (len(not_commented_lines) > 0):
                            sections.append({"type": "cell_magic", "name": magic_section_name, "body": "".join(section_lines)})
                        section_lines = []

                else:
                    section_lines.append(line)

                previous_line = line

            if len(section_lines) > 0:
                if magic_section_name == Constants.MAGIC_NAME:
                    not_commented_lines = [1 for seg_line in section_lines if not seg_line.lstrip().startswith("//")]
                    if (len(not_commented_lines) > 0):
                        sections.append({"type": "cell_magic", "name": magic_section_name, "body": "".join(section_lines)})
                else:
                    sections.append({"type": "cell_magic", "name": magic_section_name, "body": "".join(section_lines)})

            if len(sections) > 0:
                last_query = sections[-1].get("body").strip()
                if last_query == ";":
                    suppress_all_results = True
                    sections = sections[:-1]

            if len(sections) == 0:
                sections.append({"type": "cell_magic", "name": Constants.MAGIC_NAME, "body": ""})

        else:
            sections.append({"type": "line_magic", "name": Constants.MAGIC_NAME, "body": code.strip()})

        #
        # parse code to kql and options
        #
        parsed_sections = []
        last_connection_string = ""
        for section in sections:
            parsed_section = cls._parse_one_section(section, is_cell, config, engines, user_ns)
            connection_string = parsed_section.get("connection_string")
            if connection_string:
                last_connection_string = connection_string
            elif len(parsed_section.get("command")) == 0:
                parsed_section["connection_string"] = last_connection_string
            if suppress_all_results:
                parsed_section.get("options")["suppress_results"] = True
            
            parsed_sections.append(parsed_section)

        if len(parsed_sections) > 0:
            parsed_sections[-1]["last_query"] = True

        return parsed_sections


    @classmethod
    def _parse_one_section(cls, section:Dict[str,str], is_cell:bool, config:Configurable, engines:List[Engine], user_ns:Dict[str,Any])->Dict[str,Any]:
        """Separate input into (connection info, KQL statements, options)"""

        cell, command = cls._parse_kql_command(section, user_ns)
        command_name = command.get("command")
        if command_name is not None and command_name != "submit":
            cell_rest, options = cls._parse_kql_options(cell.strip(), is_cell, config, user_ns)
            cell_rest = cls._update_kql_command_params(command, cell_rest, user_ns)
            cls._validate_kql_command_params(command)
            if cell_rest: 
                raise ValueError(f"command --{command_name} has too many parameters")
            parsed_query = {"connection_string": "", "query": "", "options": options, "command": command}
            return parsed_query

        # split to max 2 parts. First part, parts[0], is the first string.
        # parts = [part.strip() for part in cell.split(None, 1)]
        parts = split_lex(cell)
        
        # print(parts)
        if not parts:
            kql, options = cls._parse_kql_options("", is_cell, config, user_ns)
            parsed_query = {"connection_string": "", "query": kql, "options": options, "command": {}}
            return parsed_query

        #
        # replace substring of the form $name or ${name}, in windows also %name% if found in env variabes
        #

        connection_string = None

        conn_str = parts[0].strip()
        if not conn_str.startswith(('-', '+')):
            _was_quoted, conn_str = strip_if_quoted(conn_str)

            #
            # connection taken from a section in  dsn file (file name have to be define in config.dsn_filename or specified as a parameter)
            #
            if is_collection(conn_str, "["):
                section = conn_str[1:-1].strip()

                # parse to get flag, for the case that the file nema is specified in the options
                code = cell[len(parts[0]):]
                kql, options = cls._parse_kql_options(code, is_cell, config, user_ns)

                parser = CP.ConfigParser()
                dsn_filename = adjust_path(options.get("dsn_filename", config.dsn_filename))
                parser.read(dsn_filename)
                cfg_dict = dict(parser.items(section))

                cfg_dict_lower = {k.lower().replace("_", "").replace("-", ""): v for (k, v) in cfg_dict.items()}
                for e in engines:
                    if e.get_mandatory_key() in cfg_dict_lower.keys():
                        all_keys = set(itertools.chain(*e.get_valid_keys_combinations()))
                        connection_kv = [f"{k}='{v}'" for k, v in cfg_dict_lower.items() if v and k in all_keys]
                        connection_string = f"{e.get_uri_schema_name()}://{';'.join(connection_kv)}"
                        break

            #
            # connection specified starting with one of the supported prefixes
            #
            elif "://" in conn_str:
                sub_parts = conn_str.strip().split("://", 1)
                if (len(sub_parts) == 2 and sub_parts[0].lower().replace("_", "").replace("-", "") in list(itertools.chain(*[e._ALT_URI_SCHEMA_NAMES for e in engines]))):
                    connection_string = conn_str
            #
            # connection specified as database@cluster
            #
            elif "@" in conn_str and "|" not in conn_str and "'" not in conn_str and '"' not in conn_str:
                connection_string = conn_str
        #
        # connection not specified, override default
        #
        if connection_string is None:
            connection_string = ""
            code = cell
        else:
            code = cell[len(parts[0]):]


        #
        # parse code to kql and options
        #

        kql, options = cls._parse_kql_options(code.strip(), is_cell, config, user_ns)
        kql = options.pop("query", None) or kql
        connection_string = options.pop("conn", None) or connection_string

        parsed_query = {"connection_string": connection_string.strip(), "query": kql, "options": options, "command": {}}

        return parsed_query


    @classmethod
    def parse_old(cls, line:str, cell:str, config:Configurable, engines:List[Engine], user_ns:Dict[str,Any])->List[Dict[str,Any]]:
        """Separate input into (connection info, KQL statements, options)"""

        is_cell = cell is not None
        cell = f"{line}\n{cell or ''}"
        cell = cell.strip()
        parsed_queries = []
        cell, command = cls._parse_kql_command(cell, user_ns)
        command_name = command.get("command")
        if command_name is not None and command_name != "submit":
            cell_rest, options = cls._parse_kql_options(cell.strip(), is_cell, config, user_ns)
            cell_rest = cls._update_kql_command_params(command, cell_rest, user_ns)
            cls._validate_kql_command_params(command)
            if cell_rest: 
                raise ValueError(f"command --{command_name} has too many parameters")
            parsed_queries.append({"connection_string": "", "query": "", "options": options, "command": command})
            return parsed_queries

        # split to max 2 parts. First part, parts[0], is the first string.
        # parts = [part.strip() for part in cell.split(None, 1)]
        parts = split_lex(cell)
        
        # print(parts)
        if not parts:
            kql, options = cls._parse_kql_options("", is_cell, config, user_ns)
            parsed_queries.append({"connection_string": "", "query": kql, "options": options, "command": {}})
            return parsed_queries

        #
        # replace substring of the form $name or ${name}, in windows also %name% if found in env variabes
        #

        connection_string = None

        conn_str = parts[0].strip()
        if not conn_str.startswith(('-', '+')):
            _was_quoted, conn_str = strip_if_quoted(conn_str)

            #
            # connection taken from a section in  dsn file (file name have to be define in config.dsn_filename or specified as a parameter)
            #
            if is_collection(conn_str, "["):
                section = conn_str[1:-1].strip()

                # parse to get flag, for the case that the file nema is specified in the options
                code = cell[len(parts[0]):]
                kql, options = cls._parse_kql_options(code, is_cell, config, user_ns)

                parser = CP.ConfigParser()
                dsn_filename = adjust_path(options.get("dsn_filename", config.dsn_filename))
                parser.read(dsn_filename)
                cfg_dict = dict(parser.items(section))

                cfg_dict_lower = {k.lower().replace("_", "").replace("-", ""): v for (k, v) in cfg_dict.items()}
                for e in engines:
                    if e.get_mandatory_key() in cfg_dict_lower.keys():
                        all_keys = set(itertools.chain(*e.get_valid_keys_combinations()))
                        connection_kv = [f"{k}='{v}'" for k, v in cfg_dict_lower.items() if v and k in all_keys]
                        connection_string = f"{e.get_uri_schema_name()}://{';'.join(connection_kv)}"
                        break

            #
            # connection specified starting with one of the supported prefixes
            #
            elif "://" in conn_str:
                sub_parts = conn_str.strip().split("://", 1)
                if (len(sub_parts) == 2 and sub_parts[0].lower().replace("_", "").replace("-", "") in list(itertools.chain(*[e._ALT_URI_SCHEMA_NAMES for e in engines]))):
                    connection_string = conn_str
            #
            # connection specified as database@cluster
            #
            elif "@" in conn_str and "|" not in conn_str and "'" not in conn_str and '"' not in conn_str:
                connection_string = conn_str
        #
        # connection not specified, override default
        #
        if connection_string is None:
            connection_string = ""
            code = cell
        else:
            code = cell[len(parts[0]):]

        #
        # split string to queries
        #
        suppress_all_results = False
        queries:List[str] = []
        if is_cell:
            queryLines:List[str] = []
            last_line:str = None
            for last_line in code.splitlines(True):
                # note: splitlines don't remove the \n suffix, each line endswith \n 
                if last_line.isspace():
                    if len(queryLines) > 0:
                        queries.append("".join(queryLines))
                        queryLines = []
                else:
                    queryLines.append(last_line)

            if len(queryLines) > 0:
                queries.append("".join(queryLines))

            if len(queries) > 0:
                last_query = queries[-1].strip()
                if last_query == ";":
                    suppress_all_results = True
                    queries = queries[:-1]

            if len(queries) == 0:
                queries.append("")
        else:
            queries.append(code.strip())

        #
        # parse code to kql and options
        #
        for query in queries:
            kql, options = cls._parse_kql_options(query.strip(), is_cell, config, user_ns)
            kql = options.pop("query", None) or kql
            connection_string = options.pop("conn", None) or connection_string
            if suppress_all_results:
                options["suppress_results"] = True
            parsed_queries.append({"connection_string": connection_string.strip(), "query": kql, "options": options, "command": {}})

        return parsed_queries

    @classmethod
    def _parse_kql_command(cls, section:Dict[str,str], user_ns:Dict[str,Any])->Tuple[str,Dict[str,Any]]:
        code = section.get("body")
        if section.get("name") != Constants.MAGIC_NAME:
            name = section.get("name").replace("_", "").replace("-", "")
            if name in ["py", "pyro", "pyrw"]:
                obj = COMMANDS_TABLE.get(name)
                return ("", {"command": obj.get("flag"), "obj": obj, "params": [section.get("body"), name]})
            else:
                # line/cell magic
                name = section.get("type")
                obj = COMMANDS_TABLE.get(name.replace("_", ""))
                command_name = obj.get("flag")

                params = []
                body = section.get("body")
                if command_name == "cell_magic":
                    first_line = body.split("\n",1)[0]
                    params.append(first_line.strip())
                    body = body[len(first_line) + 1:]

                params.append(body)
                params.append(section.get("name"))

                return ("", {"command": command_name, "obj": obj, "params": params})

        # kql section
        lookup_key = None
        trimmed_code = code
        skip_words_count = 0
        obj = None
        params_type = None
        command_name = None
        params = []
        words = code.split()
        more_words_count = len(words)
        for word in words:
            more_words_count -= 1

            if params_type == "not_quoted_str" and not word.startswith("-"):
                # break
                pass

            if skip_words_count == 0:
                _comment, skip_words_count = cls._parse_comment(word, trimmed_code)

            if skip_words_count > 0:
                skip_words_count -= 1
                trimmed_code = trimmed_code[trimmed_code.find(word) + len(word):]
                continue

            # command 
            elif command_name is None:
                if not word.strip().startswith("--"):
                    break
                word = word[2:]
                if word.startswith("-"):
                    raise ValueError(f"unknown command --{word}, commands' prefix should be a double hyphen-minus, not a triple hyphen-minus")
                lookup_key = word.lower().replace("_", "").replace("-", "")
                obj = COMMANDS_TABLE.get(lookup_key)
                if obj is None:
                    raise ValueError(f"unknown command --{word}")
                command_name = obj.get("flag")
                trimmed_code = trimmed_code[trimmed_code.find(word) + len(word):]

                params_type = obj.get("type")

                if params_type is None:
                    break
            # option
            elif word.startswith("-"):
                break

            # command's parameters
            else:
                command = {"command": command_name, "obj": obj, "params": params}
                EXIT_ON_OPTION = True
                trimmed_code, params = cls._parse_kql_command_params(command, trimmed_code, EXIT_ON_OPTION, user_ns)
                break

        if command_name is None:
            return (code.strip(), {})

        if command_name == "python":
            params = params or [""]
            params.append(lookup_key) # type of python "py", "pyro", "pyrw"

        elif command_name in ["line_magic", "cell_magic"]:
            body = params[0] if params else ""
            magic_word = body.split(None, 1)[0]
            body = body.lstrip()[len(magic_word):]

            params = []
            if command_name == "cell_magic":
                first_line = body.split("\n", 1)[0]
                params.append(first_line.strip())
                body = body[len(first_line) + 1:]
            else:
                body = body.lstrip()
            params.append(body)

            ipykernel_prefix_len = 0
            if command_name == "cell_magic" and magic_word.startswith(Constants.IPYKERNEL_CELL_MAGIC_PREFIX):
                ipykernel_prefix_len = len(Constants.IPYKERNEL_CELL_MAGIC_PREFIX)
            elif command_name == "line_magic" and magic_word.startswith(Constants.IPYKERNEL_LINE_MAGIC_PREFIX):
                ipykernel_prefix_len = len(Constants.IPYKERNEL_LINE_MAGIC_PREFIX)
            magic_name = magic_word[ipykernel_prefix_len:]
            params.append(magic_name) # the magic name

            if magic_name in ["py", "pyro", "pyrw"]:
                obj = COMMANDS_TABLE.get(magic_name)
                body = params[0] if command_name == "line_magic" else f"{params[0]}\n{params[1]}"
                return ("", {"command": obj.get("flag"), "obj": obj, "params": [body, magic_name]})

        return (trimmed_code.strip(), {"command": command_name, "obj": obj, "params": params})


    @classmethod
    def _parse_kql_command_params(cls, command:dict, code:str, exit_on_options:bool, user_ns:Dict[str,Any])->Tuple[str,List[str]]:
        trimmed_code = code.strip()
        params = command.get("params")
        obj = command.get("obj")
        params_type = obj.get("type")
        if params_type == "not_quoted_str":
            if exit_on_options and trimmed_code.startswith("-"):
                # DO NOTHING, EXIT
                pass
            else:
                params.append(trimmed_code)
                trimmed_code = ""
        elif params_type is not None:
            skip_words_count = 0
            command_name = command.get("command")
            words = code.split()
            for word in words:
                if skip_words_count == 0:
                    _comment, skip_words_count = cls._parse_comment(word, trimmed_code)

                if skip_words_count > 0:
                    skip_words_count -= 1
                    trimmed_code = trimmed_code[trimmed_code.find(word) + len(word):]
                    continue

                # option
                if exit_on_options and word.startswith("-"):
                    break

                # command's parameters

                if params_type == "str" and word[0] in ["'",'"']:
                    quoted_string, skip_words_count = cls.parse_quote(trimmed_code)
                    param = quoted_string
                    skip_words_count -= 1
                else:
                    param = word
                params.append(cls._parse_value("command", obj, command_name, param, user_ns))
                trimmed_code = trimmed_code[trimmed_code.find(word) + len(word):]

        return trimmed_code.strip(), params


    @classmethod
    def _update_kql_command_params(cls, command:dict, cell_rest:str, user_ns:Dict[str,Any])->str:
        params = command.get("params")
        if len(params) == 0:
            obj = command.get("obj")
            if obj.get("type") == "not_quoted_str":
                params.append(cell_rest)
                cell_rest = ""

            elif len(cell_rest) > 0 and obj.get("type") is not None:
                DONT_EXIT_ON_OPTION = False
                cell_rest, params = cls._parse_kql_command_params(command, cell_rest, DONT_EXIT_ON_OPTION, user_ns)
                cell_rest = ""
            elif "default" in obj:
                params.append(obj.get("default"))
            command["params"] = params

        return cell_rest


    @classmethod
    def _validate_kql_command_params(cls, command:dict):
        params = command.get("params")
        command_name = command.get("command")
        obj = command.get("obj")
        _type = obj.get("type")

        max_params = obj.get("max_params") or (1 if _type is not None else 0)

        if len(params) > max_params:
            raise ValueError(f"command --{command_name} has too many parameters")

        min_params = obj.get("min_params") or (1 if _type is not None else 0)

        if len(params) < min_params:
            raise ValueError(f"command --{command_name} is missing parameter")


    @classmethod
    def validate_query_properties(cls, schema:str, properties:Dict[str,Any])->None:
        if type(properties) == dict:
            usupported_properties = []
            for p in properties:
                prop = cls._QUERY_PROPERTIES_TABLE[p]
                prop_schema_list = prop.get("schema")
                if type(prop_schema_list) == list and schema not in prop_schema_list and len(prop_schema_list) > 0 and schema is not None:
                    usupported_properties.append(p)
                if len(usupported_properties) > 0:
                    raise ValueError(f"query properties {usupported_properties} are not supported by current connection")


    _QUERY_PROPERTIES_TABLE = {
        # NOT DOCUMENTED - (OptionBlockSplittingEnabled): Enables splitting of sequence blocks after aggregation operator. [Boolean]
        "block_splitting_enabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # NOT DOCUMENTED - (OptionDatabasePattern): Database pattern overrides database name and picks the 1st database that matches the pattern. 
        # '*' means any database that user has access to. [String]
        "database_pattern": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # NOT DOCUMENTED - If set, don't fuse projection into ExternalData operator. [bool]
        "debug_query_externaldata_projection_fusion_disabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # NOT DOCUMENTED - The percentage of threads to fanout execution to for external data nodes. [int]
        "debug_query_fanout_threads_percent_external_data":  {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},
        # (OptionDeferPartialQueryFailures): If true, disables reporting partial query failures as part of the result set. [Boolean]
        "deferpartialqueryfailures": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionMaterializedViewShuffleQuery): A hint to use shuffle strategy for materialized views that are referenced in the query. The property is an array of materialized views names and the shuffle keys to use. examples: 'dynamic([ { "Name": "V1", "Keys" : [ "K1", "K2" ] } ])' (shuffle view V1 by K1, K2) or 'dynamic([ { "Name": "V1" } ])' (shuffle view V1 by all keys) [dynamic]
        "materialized_view_shuffle": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "dict"},

        # (OptionMaxMemoryConsumptionPerQueryPerNode): Overrides the default maximum amount of memory a whole query may allocate per node. [UInt64]
        "max_memory_consumption_per_query_per_node": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionMaxMemoryConsumptionPerIterator): Overrides the default maximum amount of memory a query operator may allocate. [UInt64]
        "maxmemoryconsumptionperiterator": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionMaxOutputColumns): Overrides the default maximum number of columns a query is allowed to produce. [Long]
        "maxoutputcolumns": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionNoRequestTimeout): Enables setting the request timeout to its maximum value. [Boolean]
        "norequesttimeout": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionNoTruncation): Enables suppressing truncation of the query results returned to the caller. [Boolean]
        "notruncation": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionPushSelectionThroughAggregation): If true, push simple selection through aggregation [Boolean]
        "push_selection_through_aggregation": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # NOT DOCUMENTED - (OptionAdminSuperSlackerMode): If true, delegate execution of the query to another node [Boolean]
        "query_admin_super_slacker_mode": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (QueryBinAutoAt): When evaluating the bin_auto() function, the start value to use. [LiteralExpression]
        "query_bin_auto_at": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (QueryBinAutoSize): When evaluating the bin_auto() function, the bin size value to use. [LiteralExpression]
        "query_bin_auto_size": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (OptionQueryCursorAfterDefault): The default parameter value of the cursor_after() function when called without parameters. [string]
        "query_cursor_after_default": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # NOT DOCUMENTED - (OptionQueryCursorAllowReferencingStreamingIngestionTables): Enable usage of cursor functions over databases which have streaming ingestion enabled. [boolean]
        "query_cursor_allow_referencing_streaming_ingestion_tables": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionQueryCursorBeforeOrAtDefault): The default parameter value of the cursor_before_or_at() function when called without parameters. [string]
        "query_cursor_before_or_at_default": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (OptionQueryCursorCurrent): Overrides the cursor value returned by the cursor_current() or current_cursor() functions. [string]
        "query_cursor_current": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (OptionQueryCursorDisabled): Disables usage of cursor functions in the context of the query. [boolean]
        "query_cursor_disabled ": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionQueryCursorScopedTables): List of table names that should be scoped to cursor_after_default .. 
        # cursor_before_or_at_default (upper bound is optional). [dynamic]
        "query_cursor_scoped_tables": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "dict"},

        # (OptionQueryDataScope): Controls the query's datascope -- whether the query applies to all data or just part of it. ['default', 'all', or 'hotcache']
        "query_datascope": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "enum", "values": ['default', 'all', 'hotcache']},

        # (OptionQueryDateTimeScopeColumn): Controls the column name for the query's datetime scope (query_datetimescope_to / query_datetimescope_from). [String]
        "query_datetimescope_column": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (OptionQueryDateTimeScopeFrom): Controls the query's datetime scope (earliest) 
        # used as auto-applied filter on query_datetimescope_column only (if defined). [DateTime]
        "query_datetimescope_from": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (OptionQueryDateTimeScopeTo): Controls the query's datetime scope (latest)
        # used as auto-applied filter on query_datetimescope_column only (if defined). [DateTime]
        "query_datetimescope_to": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (OptionQueryDistributionNodesSpanSize): If set, controls the way sub-query merge behaves: 
        # the executing node will introduce an additional level in the query hierarchy for each sub-group of nodes; the size of the sub-group is set by this option. [Int]
        "query_distribution_nodes_span": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "int"},

        # (OptionQueryFanoutNodesPercent): The percentage of nodes to fanour execution to. [Int]
        "query_fanout_nodes_percent": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionQueryFanoutThreadsPercent): The percentage of threads to fanout execution to. [Int]
        "query_fanout_threads_percent": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionQueryForceRowLevelSecurity): If specified, forces Row Level Security rules, even if row_level_security policy is disabled [Boolean]
        "query_force_row_level_security": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionQueryLanguage): Controls how the query text is to be interpreted. ['csl','kql' or 'sql']
        "query_language": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "enum", "values": ['csl', 'kql', 'sql']},

        # NOT DOCUMENTED - (RemoteMaterializeOperatorInCrossCluster): Enables remoting materialize operator in cross cluster query.
        "query_materialize_remote_subquery": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionMaxEntitiesToUnion): Overrides the default maximum number of columns a query is allowed to produce. [Long]
        "query_max_entities_in_union": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionQueryNow): Overrides the datetime value returned by the now(0s) function. [DateTime]
        # note: cannot be relative to now()
        "query_now": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (OptionDebugPython): If set, generate python debug query for the enumerated python node (default first). [Boolean or Int]
        "query_python_debug": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionQueryResultsApplyGetSchema): If set, retrieves the schema of each tabular data in the results of the query instead of the data itself. [Boolean]
        "query_results_apply_getschema": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionQueryResultsCacheMaxAge): If positive, controls the maximum age of the cached query results that Kusto is allowed to return [TimeSpan]
        "query_results_cache_max_age": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},
        
        # NOT DOCUMENTED - (CostBasedOptimizerBroadcastJoinBuildMax): Max Rows count for build in broadcast join.
        "query_optimization_broadcast_build_maxSize": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # NOT DOCUMENTED - (CostBasedOptimizerBroadcastJoinProbeMin): Min Rows count for probe in broadcast join.
        "query_optimization_broadcast_probe_minSize": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # NOT DOCUMENTED - (CostBasedOptimizer): Enables automatic optimizations.
        "query_optimization_costbased_enabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # NOT DOCUMENTED - (OptionOptimizeInOperator): Optimizes in operands serialization.
        "query_optimization_in_operator": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # NOT DOCUMENTED - (CostBasedOptimizerShufflingCardinalityThreshold): Shuffling Cardinality Threshold.
        "query_optimization_shuffling_cardinality": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # NOT DOCUMENTED - (OptionQueryRemoteEntitiesDisabled): If set, queries cannot access remote databases / clusters. [Boolean]
        "query_remote_entities_disabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # NOT DOCUMENTED - (RemoteInOperandsInQuery): Enables remoting in operands.
        "query_remote_in_operands": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionProgressiveQueryMinRowCountPerUpdate): Hint for Kusto as to how many records to send in each update 
        # (Takes effect only if OptionProgressiveQueryIsProgressive is set)
        "query_results_progressive_row_count": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionProgressiveProgressReportPeriod): Hint for Kusto as to how often to send progress frames (Takes effect only if OptionProgressiveQueryIsProgressive is set)
        "query_results_progressive_update_period": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionTakeMaxRecords): Enables limiting query results to this number of records. [Long]
        "query_take_max_records": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionQueryConsistency): Controls query consistency. ['strongconsistency' or 'normalconsistency' or 'weakconsistency']
        "queryconsistency": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "enum", "values": ['strongconsistency', 'normalconsistency', 'weakconsistency']},

        # (OptionRequestBlockRowLevelSecurity): If specified, blocks access to tables for which row_level_security policy is enabled [Boolean]
        "request_block_row_level_security": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionRequestCalloutDisabled): If set, callouts to external services are blocked. [Boolean]
        "request_callout_disabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionRequestDescription): Arbitrary text that the author of the request wants to include as the request description. [String]
        "request_description": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (OptionRequestExternalTableDisabled): If specified, indicates that the request cannot invoke code in the ExternalTable. [bool]
        "request_external_table_disabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionDoNotImpersonate): If specified, indicates that the service shouldn't impersonate the caller's identity. [bool]
        "request_impersonation_disabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionRequestReadOnly): If specified, indicates that the request must not be able to write anything. [Boolean]
        "request_readonly": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionRequestEntitiesDisabled): If specified, indicates that the request cannot access remote databases and clusters. [bool]
        "request_remote_entities_disabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionRequestSandboxedExecutionDisabled): If specified, indicates that the request cannot invoke code in the sandbox. [bool]
        "request_sandboxed_execution_disabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # NOT DOCUMENTED - (OptionResponseDynamicSerialization): Controls the serialization of 'dynamic' values in result sets. ['string', 'json']
        "response_dynamic_serialization": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "enum", "values": ['string', 'json']},

        # NOT DOCUMENTED - (OptionResponseDynamicSerialization_2): Controls the serialization of 'dynamic' string and null values in result sets. ['legacy', 'current']
        "response_dynamic_serialization_2": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "enum", "values": ['legacy', 'current']},

        # (OptionResultsProgressiveEnabled): If set, enables the progressive query stream
        "results_progressive_enabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # NOT DOCUMENTED - (OptionSandboxedExecutionDisabled): If set, using sandboxes as part of query execution is disabled. [Boolean]
        "sandboxed_execution_disabled": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},

        # (OptionServerTimeout): Overrides the default request timeout. [TimeSpan]
        # is capped by 1hour
        "servertimeout": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "str"},

        # (OptionTruncationMaxRecords): Overrides the default maximum number of records a query is allowed to return to the caller (truncation). [Long]
        "truncationmaxrecords": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionTruncationMaxSize): Overrides the dfefault maximum data size a query is allowed to return to the caller (truncation). [Long]
        "truncationmaxsize": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "uint"},

        # (OptionValidatePermissions): Validates user's permissions to perform the query and doesn't run the query itself. [Boolean]
        "validate_permissions": {"schema": [Schema.AZURE_DATA_EXPLORER], "type": "bool"},


        # For either implicit or explicit cross-application queries, specify resources you will be accessing
        # see https://dev.loganalytics.io/documentation/Using-the-API/Cross-Resource-Queries
        "workspaces": {"schema": [Schema.LOG_ANALYTICS], "type": "list"},

        # For either implicit or explicit cross-application queries, specify resources you will be accessing
        # see: https://dev.applicationinsights.io/documentation/Using-the-API/Cross-Resource-Queries
        "applications": {"schema": [Schema.APPLICATION_INSIGHTS, Schema.AIMON], "type": "list"},

        # The timespan over which to query data. This is an ISO8601 time period value. This timespan is applied in addition to any that are specified in the query expression.
        # see: https://docs.microsoft.com/en-us/rest/api/application-insights/query/get
        "timespan": {"schema": [Schema.APPLICATION_INSIGHTS, Schema.AIMON, Schema.LOG_ANALYTICS], "type": "iso8601_duration"},
    }


    # all lookup keys in table, must be without spaces, underscores and hypthen-minus, because parser ignores them
    _OPTIONS_TABLE:Dict[str,Dict[str,Any]] = {
        "ad": {"abbreviation": "autodataframe"},
        "autodataframe": {"flag": "auto_dataframe", "type": "bool"},
        "se": {"abbreviation": "shorterrors"},
        "shorterrors": {"flag": "short_errors", "type": "bool"},
        "f": {"abbreviation": "feedback"},
        "feedback": {"flag": "feedback", "type": "bool"},
        "sci": {"abbreviation": "showconninfo"},
        "showconninfo": {"flag": "show_conn_info", "type": "str", "allow_none": True},
        "c2lv": {"abbreviation": "columnstolocalvars"},
        "columnstolocalvars": {"flag": "columns_to_local_vars", "type": "bool"},
        "sqt": {"abbreviation": "showquerytime"},
        "showquerytime": {"flag": "show_query_time", "type": "bool"},
        "sq": {"abbreviation": "showquery"},
        "showquery": {"flag": "show_query", "type": "bool"},
        "sql": {"abbreviation": "showquerylink"},
        "showquerylink": {"flag": "show_query_link", "type": "bool"},
        "qld": {"abbreviation": "querylinkdestination"},
        "querylinkdestination": {"flag": "query_link_destination", "type": "str"},

        "esr": {"abbreviation": "enablesuppressresult"},
        "enablesuppressresult": {"flag": "enable_suppress_result", "type": "bool"},
        "pfi": {"abbreviation": "plotlyfsincludejs"},
        "plotlyfsincludejs": {"flag": "plotly_fs_includejs", "type": "bool"},
        "pw": {"abbreviation": "popupwindow"},
        "popupwindow": {"flag": "popup_window", "type": "bool", "init": False},
        "al": {"abbreviation": "autolimit"},
        "autolimit": {"flag": "auto_limit", "type": "int", "allow_none": True},
        "dl": {"abbreviation": "displaylimit"},
        "displaylimit": {"flag": "display_limit", "type": "int", "allow_none": True},
        "wait": {"abbreviation": "timeout"},
        "to": {"abbreviation": "timeout"},
        "timeout": {"flag": "timeout", "type": "int", "allow_none": True},
        "ptst": {"abbreviation": "prettytablestyle"},
        "prettytablestyle": {"flag": "prettytable_style", "type": "str"},
        "var": {"abbreviation": "lastrawresultvar"},
        "lastrawresultvar": {"flag": "last_raw_result_var", "type": "str"},
        "tp": {"abbreviation": "tablepackage"},
        "tablepackage": {"flag": "table_package", "type": "str"},
        "pp": {"abbreviation": "plotpackage"},
        "plotpackage": {"flag": "plot_package", "type": "str"},
        "df": {"abbreviation": "dsnfilename"},
        "dsnfilename": {"flag": "dsn_filename", "type": "str", "allow_none": True},
        "vc": {"abbreviation": "validateconnectionstring"},
        "validateconnectionstring": {"flag": "validate_connection_string", "type": "bool"},
        "aps": {"abbreviation": "autopopupschema"},
        "autopopupschema": {"flag": "auto_popup_schema", "type": "bool"},
        "jd": {"abbreviation": "jsondisplay"},
        "jsondisplay": {"flag": "json_display", "type": "str"},
        "sjd": {"abbreviation": "schemajsondisplay"},
        "schemajsondisplay": {"flag": "schema_json_display", "type": "str"},
        "pd": {"abbreviation": "palettedesaturation"},
        "palettedesaturation": {"flag": "palette_desaturation", "type": "float"},
        "pn": {"abbreviation": "palettename"},
        "paramsdict": {"flag": "params_dict", "type": "dict", "init": None},
        "palettename": {"flag": "palette_name", "type": "str"},
        "cache": {"flag": "cache", "type": "str", "allow_none": True},
        "usecache": {"flag": "use_cache", "type": "str", "allow_none": True},
        
        "tempfoldername": {"flag": "temp_folder_name", "type": "str"},
        "cachefoldername": {"flag": "cache_folder_name", "type": "str"},
        "exportfoldername": {"flag": "export_folder_name", "type": "str"},
        "addkqlreftohelp": {"flag": "add_kql_ref_to_help", "type": "bool"},
        "addschematohelp": {"flag": "add_schema_to_help", "type": "bool"},
        "enableadditemstohelp": {"flag": "enable_add_items_to_help", "type": "bool"},
        "notebookapp": {"flag": "notebook_app", "type": "str"},
        "debug": {"flag": "debug", "type": "bool"},

        "checkmagicversion": {"flag": "check_magic_version", "type": "bool"},
        "showwhatnew": {"flag": "show_what_new", "type": "bool"},
        "showinitbanner": {"flag": "show_init_banner", "type": "bool"},
        "iskernelintializtion": {"flag": "is_kernel_intializtion", "type": "bool"},
        "warnmissingdependencies": {"flag": "warn_missing_dependencies", "type": "bool"},
        "warnmissingenvvariables": {"flag": "warn_missing_env_variables", "type": "bool"},
        "allowsinglelinecell": {"flag": "allow_single_line_cell", "type": "bool"},
        "allowpycommentsbeforecell": {"flag": "allow_py_comments_before_cell", "type": "bool"},
        
        "kqlmagickernel": {"flag": "kqlmagic_kernel", "type": "bool"},

        "extrasrequire": {"flag": "extras_require", "type": "str"},

        "testnotebookapp": {"flag": "test_notebook_app", "type": "str"},

        "cloud": {"flag": "cloud", "type": "str"},
        "enablesso": {"flag": "enable_sso", "type": "bool"},
        "ssodbgcinterval": {"flag": "sso_db_gc_interval", "type": "int"},
        
        "authusehttpclient": {"flag": "auth_use_http_client", "type": "bool"},

        "tryazclilogin": {"flag": "try_azcli_login", "type": "bool"},
        "tryazcliloginbyprofile": {"flag": "try_azcli_login_by_profile", "type": "bool"},
        "tryvscodelogin": {"flag": "try_vscode_login", "type": "bool"},
        "tryazcliloginsubscription": {"flag": "try_azcli_login_subscription", "type": "str", "allow_none": True},
        "trytoken": {"flag": "try_token", "type": "dict", "allow_none": True},
        "trymsi": {"flag": "try_msi", "type": "dict", "allow_none": True},

        "idtag": {"abbreviation": "requestidtag"},
        "requestidtag": {"flag": "request_id_tag", "type": "str", "allow_none": True},

        "apptag": {"abbreviation": "requestapptag"},
        "requestapptag": {"flag": "request_app_tag", "type": "str", "allow_none": True},

        "usertag": {"abbreviation": "requestusertag"},
        "requestusertag": {"flag": "request_user_tag", "type": "str", "allow_none": True},
        
        "uatag": {"abbreviation": "requestuseragenttag"},
        "useragenttag": {"abbreviation": "requestuseragenttag"},
        "requestuatag": {"abbreviation": "requestuseragenttag"},
        "requestuseragenttag": {"flag": "request_user_agent_tag", "type": "str", "allow_none": True},

        "maxage": {"abbreviation": "requestcachemaxage"},
        "requestcachemaxage": {"flag": "request_cache_max_age", "type": "int", "allow_none": True},

        "dcln": {"abbreviation": "devicecodeloginnotification"},
        "devicecodeloginnotification": {"flag": "device_code_login_notification", "type": "str"},

        "dcne": {"abbreviation": "devicecodenotificationemail"},
        "devicecodenotificationemail": {"flag": "device_code_notification_email", "type": "str"},

        "saveas": {"flag": "save_as", "type": "str", "init": None},
        "saveto": {"flag": "save_to", "type": "str", "init": None},
        "query": {"flag": "query", "type": "str", "init": None},
        "conn": {"flag": "conn", "type": "str", "init": None},
        "queryproperties": {"flag": "query_properties", "type": "dict", "init": None},

        "pc": {"abbreviation": "palettecolors"},
        "palettecolors": {"flag": "palette_colors", "type": "int"},
        "pr": {"abbreviation": "palettereverse"},
        "palettereverse": {"flag": "palette_reverse", "type": "bool", "init": False},

        "ps": {"abbreviation": "popupschema"},
        "popupschema": {"flag": "popup_schema", "type": "bool", "init": False},

        "did": {"abbreviation": "displayid"},
        "displayid": {"flag": "display_id", "type": "bool", "init": False},
        "displayhandlers": {"flag": "display_handlers", "type": "dict", "init": {}},

        "pi": {"abbreviation": "popupinteraction"},        
        "popupinteraction": {"flag": "popup_interaction", "type": "str"},
        "tempfilesserver": {"flag": "temp_files_server", "type": "str"},
        "tempfilesserveraddress": {"flag": "temp_files_server_address", "type": "str", "allow_none": True},
        
        "kernellocation": {"flag": "kernel_location", "type": "str"},
        "kernelid": {"flag": "kernel_id", "type": "str", "allow_none": True},

        "notebookserviceaddress": {"flag": "notebook_service_address", "type": "str", "allow_none": True},

        "dtd": {"abbreviation": "dynamictodataframe"},
        "dynamictodataframe": {"flag": "dynamic_to_dataframe", "type": "str"},

        "tempfolderlocation": {"flag": "temp_folder_location", "type": "str"},

        "pl": {"abbreviation": "plotlylayout"},
        "plotlylayout": {"flag": "plotly_layout", "type": "dict", "allow_none": True},

        "atw": {"abbreviation": "authtokenwarnings"},
        "authtokenwarnings": {"flag": "auth_token_warnings", "type": "bool"},

        "ecbp": {"abbreviation": "enablecurlybracketsparams"},
        "enablecurlybracketsparams": {"flag": "enable_curly_brackets_params", "type": "bool"},

        "nop": {"flag": "nop", "type": "bool", "init": False},  # does nothing, useful to indicate option part when no options are required

        "av": {"abbreviation": "assignvar"},
        "assignvar": {"flag": "assign_var", "type": "str", "allow_none": True},
        "cv": {"abbreviation": "cursorvar"},
        "cursorvar": {"flag": "cursor_var", "type": "str", "allow_none": True},
        "ismagic": {"flag": "is_magic", "type": "bool"},

        "caim": {"abbreviation": "codeauthinteractivemode"},
        "codeauthinteractivemode": {"flag": "code_auth_interactive_mode", "type": "str", "allow_none": True},
    }



    @classmethod
    def validate_override(cls, name:str, config:Configurable, **override_options)->Dict[str,Any]:
        """validate the provided option are valid"""

        options = {}
        for key, value in override_options.items():
            obj = cls._get_obj(key, allow_abbr=True)
            if obj.get("flag") in config.read_only_trait_names:
                raise ValueError(f"option '{key}' in {name} is readony, cannot be set")
            cls._convert(name, obj, key, value)
            cls._validate_config_trait(name, obj, key, value, config)
            options[obj.get("flag")] = value

        return options


    @classmethod
    def parse_option(cls, dict_name:str, key:str, value:str, config:Configurable=None, lookup:Dict[str,Dict[str,Any]]=None, user_ns:Dict[str,Any]=None, allow_abbr:bool=None, force:bool=False):
        """validate the provided option are valid
           return normalized key and value"""

        obj = cls._get_obj(key, lookup=lookup, allow_abbr=allow_abbr)
        value = cls._parse_value(dict_name, obj, key, value, user_ns=user_ns)
        cls._validate_config_trait(dict_name, obj, key, value, config)
        key_name = obj.get("flag", key)
        if config.is_read_only(key_name) and value != getattr(config, key_name):
            # done to raise the proer error
            setattr(config, key_name, value)

        return key_name, value


    @classmethod
    def _parse_value(cls, dict_name:str, obj:Dict[str,Any], key:str, string:str, user_ns:Dict[str,Any])->Any:
        _type = obj.get("type")

        if string == "" and _type == "str":
            return string

        # if we allow to bring value from python, we also allow from env variables
        # when we parse env vironment with option we fon't use user_ns
        if string.startswith('$') and user_ns:
            env_var_name = string[1:]
            if not is_env_var(env_var_name):
                raise ValueError(f"failed to parse referred value, due environment variable {env_var_name} not set")
            string = get_env_var(env_var_name)
            _was_quoted, value = strip_if_quoted(string)
        else:
            try:
                value = eval(string, None, user_ns)
            except: # pylint: disable=bare-except
                # if no user_ns it means parse is for environment var, and it just may be an unquoted object
                if user_ns:
                    raise
                value = string

        # check value is of the right type
        try:
            return cls._convert(dict_name, obj, key, value)
        except: # pylint: disable=bare-except
            raise


    @classmethod
    def parse_config_key(cls, key:str, config:Configurable, allow_abbr:bool=None)->Tuple[str,str,Any]:
        """validate the provided option key is valid
           return normalized key"""

        obj = cls._get_obj(key, allow_abbr=allow_abbr) 
        name = obj.get("flag")
        if "init" in obj:
            value = obj.get("init")
        elif name in cls.traits_dict:
            value = getattr(config, name)
        else:
            raise f"internal error '{key}' has no init value and not defined as Kqlmagic traitlet"
        return name, value


    @classmethod
    def _get_obj(cls, key:str, lookup:Dict[str,Dict[str,Any]]=None, allow_abbr:bool=None)->Dict[str,Any]:
        lookup_key = key.lower().replace("-", "").replace("_", "")
        lookup_table = lookup or cls._OPTIONS_TABLE
        obj = lookup_table.get(lookup_key)
        if obj is None:
            raise ValueError(f"unknown option '{key}'")

        if obj.get("abbreviation"):
            obj = lookup_table.get(obj.get("abbreviation"))
            if not allow_abbr is not True:
                raise ValueError(f"unknown option '{key}'. (Found option abbreviation '{key}' for {obj.get('flag')})")
        return obj


    @classmethod
    def _parse_kql_options(cls, code:str, is_cell:bool, config:Configurable, user_ns:Dict[str,Any])->Tuple[str,Dict[str,Any]]:
        trimmed_kql = code
        trimmed_kql = trimmed_kql.strip()
        suppress_results = False
        if trimmed_kql.endswith(";"):
            suppress_results = not is_cell
            if is_cell:
                lines = trimmed_kql.splitlines(True)
                if lines[-1].strip() == ";":
                    suppress_results = True
            if suppress_results:
                trimmed_kql = trimmed_kql[:-1].strip()
        
        words = trimmed_kql.split()

        properties = {}

        table = options = cls.default_options.copy()

        if not words:
            return ("", options)
        num_words = len(words)
        first_word = 0

        if num_words - first_word >= 2 and words[first_word + 1] == "<<":
            options["result_var"] = words[first_word]
            trimmed_kql = trimmed_kql[trimmed_kql.find("<<") + 2:]
            first_word += 2

        obj = None
        key = None
        opt_key = None
        key_state = True
        option_type = None
        is_option = True
        is_property = False
        skip_words_count = 0
        for word in words[first_word:]:
            if key_state:

                if skip_words_count == 0:
                    _comment, skip_words_count = cls._parse_comment(word, trimmed_kql)              

                if skip_words_count > 0:
                    skip_words_count -= 1
                    trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word):]
                    continue

                is_option = word.startswith("-")
                is_property = word.startswith("+")
                option_type = "option" if is_option else "query property"
                if not is_option and not is_property:
                    break
                # validate it is not a command
                if is_option and word.startswith("--"):
                    raise ValueError(f"invalid {option_type} '{word}', cannot start with a bouble hyphen-minus")

                trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word):]
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
                    if obj.get("abbreviation") is not None:
                        obj = cls._OPTIONS_TABLE.get(obj.get("abbreviation"))
                    if obj.get("flag") in config.read_only_trait_names:
                        raise ValueError(f"{option_type} {key} is readony, cannot be set")

                    _type = obj.get("type")
                    opt_key = obj.get("flag") or lookup_key
                    if _type == "bool" and value is None:
                        table[opt_key] = bool_value
                    else:
                        if not bool_value:
                            raise ValueError(f"{option_type} {key} cannot be negated")
                        if value is not None:
                            table[opt_key] = cls._parse_value("options" if is_option else "query properties", obj, key, value, user_ns)
                        else:
                            key_state = False
                else:
                    raise ValueError(f"unknown {option_type} '{key}'")
            else:
                trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word):]
                table[opt_key] = cls._parse_value("options", obj, key, word, user_ns)
                key_state = True
            first_word += 1

            # validate using config traits
            if key_state and is_option:
                cls._validate_config_trait("options", obj, key, options.get(opt_key), config)
            
        if not key_state:
            raise ValueError(f"{option_type} '{opt_key}' must have a value")

        if options.get("query_properties"):
            properties.update(options["query_properties"])
        options["query_properties"] = properties
        if suppress_results:
            options["suppress_results"] = True
        return (trimmed_kql.strip(), options)


    @classmethod
    def _parse_comment(cls, word:str, _str:str)->Tuple[str,int]:

        comment = None
        skip_words_count = 0
        if word.startswith("//"):
            idx_start = _str.find(word)
            idx_end = _str[idx_start:].find("\n")
            if idx_end > 0:
                idx_end = idx_start + idx_end
                comment = _str[idx_start:idx_end]
            else:
                comment = _str[idx_start:]
            comment_words = comment.split()
            skip_words_count = len(comment_words)

        return comment, skip_words_count


    @classmethod
    def parse_and_get_kv_string(cls, conn_str:str, user_ns:Dict[str,Any], keep_original_key:bool=None)->Dict[str,Any]:
        rest = conn_str
        rest = rest.strip()
        _was_quoted, rest = strip_if_quoted(rest)
            
        matched_kv = {}

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
                    rest = rest[r_idx + 1:].strip()

            # key only
            elif r_idx >= 0 and r_idx < l_idx:
                if l_char == "(":
                    raise ValueError("invalid key/value string, missing left parethesis.")
                else:
                    key = rest[:r_idx].strip()
                    val = ""
                    rest = rest[r_idx + 1:].strip()

            # key and value
            else:
                key = rest[:l_idx].strip()
                rest = rest[l_idx + 1:].strip()
                r_idx = rest.find(r_char)
                if r_idx < 0:
                    if l_char == "(":
                        raise ValueError("invalid key/value string, missing right parethesis.")
                    else:
                        val = rest
                        rest = ""
                else:
                    val = rest[:r_idx].strip()
                    rest = rest[r_idx + 1:].strip()
                if extra_delimiter is not None:
                    if key.startswith(extra_delimiter):
                        key = key[1:].strip()
                    elif delimiter_required:
                        raise ValueError("invalid key/value string, missing delimiter.")
                    delimiter_required = True

            # key exist
            if len(key) > 0:
                if keep_original_key is True:
                    lookup_key = key
                else:
                    val = cls._parse_value("key/value", {"type": "str"}, key, val, user_ns)
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
    def parse_quote(cls, string:str):
        string = string.strip()
        delimiter = string[0]
        delimiter_len = 1
        triple_quote = len(string) > 2 and string[1] == delimiter and string[2] == delimiter
        if triple_quote:
            delimiter_len = 3
            delimiter = delimiter * 3
            quoted_string_len = string[3:].find(delimiter)

        else:
            escape = False
            quoted_string_len = -1

            count = 0
            for c in string[1:]:
                if c == "\\":
                    escape = not escape
                elif escape:
                    pass
                elif c == delimiter:
                    quoted_string_len = count
                    break
                count += 1

        if quoted_string_len >= 0:
            trimmed_string = string[quoted_string_len + 2 * delimiter_len:]
            if len(trimmed_string) > 0 and not trimmed_string[0].isspace():
                raise SyntaxError("invalid syntax after quoted string, should be followed by whitespace only")

            quoted_string = string[delimiter_len:quoted_string_len + delimiter_len]
            quoted_words = len(quoted_string.split())
            if len(quoted_string)> 0:
                if quoted_string[-1].isspace():
                    quoted_words += 1
                if quoted_string[0].isspace():
                    quoted_words += 1
            else:
                quoted_words = 1
            return delimiter + quoted_string + delimiter, quoted_words
        else:
            raise SyntaxError("EOL while scanning quoted string")


    @classmethod
    def _convert(cls, name:str, obj:Dict[str,Any], key:str, value:Any)->Any:
        if value is None:
            if obj.get("allow_none"):
                return None
            else:
                raise ValueError(f"option '{key}' doesn't allow None value.")
        _type = None
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
                    if value.lower() == 'true':
                        return True
                    elif value.lower() == 'false':
                        return False
                    else:
                        raise ValueError
                elif bool(value) != int(value):
                    raise ValueError
                return bool(value)
            elif _type == "dict":
                return dict(value)
            elif _type == "list":
                if type(value) == str:
                    value = [value]                     
                return list(value)
            elif _type == "enum":
                enum_values = obj.get("values", [])
                if enum_values.index(value) >= 0:
                    return value
                else:
                    raise ValueError
            elif _type == "iso8601_duration":
                # There are four ways to express a time interval:
                # Start and end, such as "2007-03-01T13:00:00Z/2008-05-11T15:30:00Z"
                # Start and duration, such as "2007-03-01T13:00:00Z/P1Y2M10DT2H30M"
                # Duration and end, such as "P1Y2M10DT2H30M/2008-05-11T15:30:00Z"
                # Duration only, such as "P1Y2M10DT2H30M", with additional context information

                value_list = [value] if type(value) != list else list(value)[:2]
                if len(value_list) == 0:
                    raise ValueError

                elif len(value_list) == 1:
                    value = value_list[0]
                    if isinstance(value, timedelta):
                        isodate = Dependencies.get_module("isodate", message="timedelta convertion to iso8601 duration format is not supported without isodate module, use instead a datetime range format, or already converted string") # will throw if does not exist
                        value = isodate.duration_isoformat(value)
                    elif type(value) != str:
                        raise ValueError
                    return value

                else:
                    start_value = value_list[0]
                    end_value = value_list[1]
                    if isinstance(start_value, timedelta):
                        isodate = Dependencies.get_module("isodate", dont_throw=True)
                        if isodate:
                           start_value = isodate.duration_isoformat(start_value) 
                        else:
                            end_datetime = end_value if isinstance(end_value, datetime) else dateutil.parser.isoparse(end_value)
                            start_value = end_datetime - start_value
                    elif isinstance(end_value, timedelta):
                        isodate = Dependencies.get_module("isodate", dont_throw=True)
                        if isodate:
                           end_value = isodate.duration_isoformat(end_value)
                        else:
                            start_datetime = end_value if isinstance(start_value, datetime) else dateutil.parser.isoparse(start_value)
                            end_value = end_value + start_datetime
                    value_list = [v.strftime('%Y-%m-%dT%H:%M:%S%ZZ') if isinstance(v, datetime) else str(v) for v in [start_value, end_value]]
                    return "/".join(value_list)

            else:
                return str(value)

        except Exception as e:
            option_type = "property" if name == "query properties" else "option"
            due_message = f"{e}" or f"invalid '{_type}' of value '{value}'"
            raise ValueError(f"failed to set {option_type} '{key}' in {name}, due to {due_message}")


    @classmethod
    def _validate_config_trait(cls, dict_name:str, obj:Dict[str,Any], key:str, value:Any, config:Configurable)->None:           
        # validate using config traits
        name = obj.get("flag")
        if isinstance(config, Configurable) and name in cls.traits_dict:
            #
            # save current value
            #
            try:
                new_value = cls._convert(dict_name, obj, key, value)
                trait:TraitType = cls.traits_dict.get(name)
                if hasattr(trait, "_validate"):
                    validated_value = trait._validate(config, new_value)
                return
            except Exception as error:
                raise ValueError(f"failed to set option '{key}' in {dict_name}, due to invalid value '{value}'. Exception: {error}")
