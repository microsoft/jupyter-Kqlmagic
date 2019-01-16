# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import itertools
import os
import six
from six.moves import configparser as CP
from Kqlmagic.log import Logger, logger
from traitlets import Bool, Int, Unicode, Enum, Float, TraitError


class Parser(object):

    @classmethod
    def parse(cls, cell, config, engines: list, user_ns: dict):
        """Separate input into (connection info, KQL statements, options)"""

        parsed_queries = []
        cell, command = cls._parse_kql_command(cell, user_ns)
        if len(command) > 0 and command.get("command") != "submit":
            cell, options = cls._parse_kql_options(cell.strip(), config, user_ns)
            if cell: 
                raise ValueError("command {0} has too many parameters".format(command.get("command")))
            parsed_queries.append({"connection": "", "query": "", "options": options, "command": command})
            return parsed_queries

         # split to max 2 parts. First part, parts[0], is the first string.
        parts = [part.strip() for part in cell.split(None, 1)]

        # print(parts)
        if not parts:
            parsed_queries.append({"connection": "", "query": "", "options": {}, "command": {}})
            return parsed_queries
        

        #
        # replace substring of the form $name or ${name}, in windows also %name% if found in env variabes
        #
        # parts[0] = os.path.expandvars(parts[0])  # for environment variables
        sub_parts = parts[0].split("://", 1)

        connection = None
        # assume connection is specified and will be found
        code = parts[1] if len(parts) == 2 else ""

        #
        # connection taken from a section in  dsn file (file name have to be define in config.dsn_filename or specified as a parameter)
        #
        if parts[0].startswith("[") and parts[0].endswith("]"):
            section = parts[0][1:-1].strip()

            # parse to get flag, for the case that the file nema is specified in the options
            kql, options = cls._parse_kql_options(code, config, user_ns)

            parser = CP.ConfigParser()
            parser.read(options.get("dsn_filename", config.dsn_filename))
            cfg_dict = dict(parser.items(section))

            cfg_dict_lower = {k.lower().replace("_", "").replace("-", ""): v for (k, v) in cfg_dict.items()}
            for e in engines:
                if e._MANDATORY_KEY in cfg_dict_lower.keys():
                    all_keys = set(itertools.chain(*e._VALID_KEYS_COMBINATIONS))
                    connection_kv = ["{0}='{1}'".format(k, v) for k, v in cfg_dict_lower.items() if v and k in all_keys]
                    connection = "{0}://{1}".format(e._URI_SCHEMA_NAME, ";".join(connection_kv))
                    break

        #
        # connection specified starting with one of the supported prefixes
        #
        elif (len(sub_parts) == 2 and not sub_parts[0].startswith("-") and 
        sub_parts[0].lower().replace("_", "").replace("-", "") in list(itertools.chain(*[e._ALT_URI_SCHEMA_NAMES for e in engines]))):

            connection = parts[0]
        #
        # connection specified as database@cluster
        #
        elif "@" in parts[0] and "|" not in parts[0] and "'" not in parts[0] and '"' not in parts[0] and " " not in parts[0]:
            connection = parts[0]
        #
        # connection not specified, override default
        #
        if connection is None:
            connection = ""
            code = cell

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
        "usage" : {"flag": "usage", "type": "bool", "init": "False"},
        "submit" : {"flag": "submit", "type": "bool", "init": "False"}, # default
        "help" : {"flag": "help", "type": "str", "init": "None", "default": "help"},
        "faq": {"flag": "faq", "type": "bool", "init": "False"},
        "palette": {"flag": "palette", "type": "bool", "init": "False"},
        "palettes": {"flag": "palettes", "type": "bool", "init": "False"},
        # should be per connection
        "cache": {"flag": "cache", "type": "str", "init": "None"},
        "usecache": {"flag": "use_cache", "type": "str", "init": "None"},
        "schema": {"flag": "schema", "type": "str", "init": "None", "default": "None"},
    }
    @classmethod
    def _parse_kql_command(cls, code, user_ns: dict):
        if not code.strip().startswith("--"):
            return (code.strip(), {})
        words = code.split()
        word = words[0][2:]
        if word.startswith("-"):
            raise ValueError("unknown command {0}, commands' prefix is a double hyphen-minus, not a triple hyphen-minus".format(words[0]))
        lookup_key = word.lower().replace("_", "").replace("-", "")
        obj = cls._COMMANDS_TABLE.get(lookup_key)
        if obj is None:
            raise ValueError("unknown command {0}".format(words[0]))

        trimmed_code = code
        trimmed_code = trimmed_code[trimmed_code.find(words[0]) + len(words[0]) :]

        _type = obj.get("type")
        if _type == "bool":
            param = True 
        elif len(words) >= 2 and not words[1].startswith("-"):
            param = cls.parse_value(words[1], words[0], _type, user_ns)
            trimmed_code = trimmed_code[trimmed_code.find(words[1]) + len(words[1]) :]
        elif obj.get("default") is not None:
            param = obj.get("default")
        else:
            raise ValueError("command {0} is missing parameter".format(word[0]))

        return (trimmed_code.strip(), {"command":  obj.get("flag"), "param": param})

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
        "pd": {"abbreviation": "palettedesaturation"},
        "palettedesaturation": {"flag": "palette_desaturation", "type": "float", "config": "config.palette_desaturation"},
        "pn": {"abbreviation": "palettename"},
        "paramsdict": {"flag": "params_dict", "type": "dict", "init": "None"},
        "palettename": {"flag": "palette_name", "type": "str", "config": "config.palette_name"},
        "cache": {"flag": "cache", "readonly": "True", "config": "config.cache"},
        "usecache": {"flag": "use_cache", "readonly": "True", "config": "config.use_cache"},
        
        "tempfoldername": {"flag": "temp_folder_name", "readonly": "True", "config": "config.temp_folder_name"},
        "cachefoldername": {"flag": "cache_folder_name", "readonly": "True", "config": "config.cache_folder_name"},
        "exportfoldername": {"flag": "export_folder_name", "readonly": "True", "config": "config.export_folder_name"},
        "addkqlreftohelp": {"flag": "add_kql_ref_to_help", "readonly": "True", "config": "config.add_kql_ref_to_help"},
        "addschematohelp": {"flag": "add_schema_to_help", "readonly": "True", "config": "config.add_schema_to_help"},
        "notebookapp": {"flag": "notebook_app", "readonly": "True", "config": "config.notebook_app"},
        "testnotebookapp": {"flag": "test_notebook_app", "readonly": "True", "config": "config.test_notebook_app"},
        "saveas": {"flag": "save_as", "type": "str", "init": "None"},
        "saveto": {"flag": "save_to", "type": "str", "init": "None"},
        "query": {"flag": "query", "type": "str", "init": "None"},
        "conn": {"flag": "conn", "type": "str", "init": "None"},

        "pc": {"abbreviation": "palettecolors"},
        "palettecolors": {"flag": "palette_colors", "type": "int", "config": "config.palette_colors"},
        "pr": {"abbreviation": "palettereverse"},
        "palettereverse": {"flag": "palette_reverse", "type": "bool", "init": "False"},

        "ps": {"abbreviation": "popupschema"},
        "popupschema": {"flag": "popup_schema", "type": "bool", "init": "False"},
    }    
    @classmethod
    def _parse_kql_options(cls, code, config, user_ns: dict):
        words = code.split()
        options = {}

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
        for word in words[first_word:]:
            if key_state:
                if not word[0].startswith("-"):
                    break
                # validate it is not a command
                if word[0].startswith("--"):
                    raise ValueError("invalid option {0}, cannot start with a bouble hyphen-minus".format(word[0]))

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
                lookup_key = key.lower().replace("-", "").replace("_", "")
                obj = cls._OPTIONS_TABLE.get(lookup_key)
                if obj is not None:
                    if obj.get("abbreviation"):
                        obj = cls._OPTIONS_TABLE.get(obj.get("abbreviation"))
                    if obj.get("readonly"):
                        raise ValueError("option {0} is readony, cannot be set".format(key))

                    _type = obj.get("type")
                    opt_key = obj.get("flag")
                    option_config = obj.get("config")
                    if _type == "bool" and value is None:
                        options[opt_key] = bool_value
                    else:
                        if not bool_value:
                            raise ValueError("option {0} cannot be negated".format(key))
                        if value is not None:
                            options[opt_key] = cls.parse_value(value, key, _type, user_ns)
                        else:
                            key_state = False
                else:
                    raise ValueError("unknown option")
            else:
                trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word) :]
                options[opt_key] = cls.parse_value(word, key, _type, user_ns)
                key_state = True
            first_word += 1

            # validate using config traits
            if key_state and option_config is not None:
                template = "'{0}'" if _type == "str" else "{0}"
                saved = eval(option_config)
                exec(option_config + "=" + (template.format(str(options[opt_key]).replace("'", "\\'")) if options[opt_key] is not None else "None"))
                exec(option_config + "=" + (template.format(str(saved).replace("'", "\\'")) if saved is not None else "None"))

        if not key_state:
            raise ValueError("last option is missing parameter")

        if num_words - first_word > 0:
            last_word = words[-1].strip()
            if last_word.endswith(";"):
                options["suppress_results"] = True
                trimmed_kql = trimmed_kql[: trimmed_kql.rfind(";")]
        return (trimmed_kql.strip(), options)

    @classmethod
    def parse_and_get_kv_string(cls, conn_str: str, user_ns: dict):

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
                val = cls.parse_value(val, key, "str", user_ns)
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
    def parse_value(cls, value: str, key: str, _type: str, user_ns: dict):

        def _convert(val, _type):
            if _type == "int":
                if float(val) != int(val):
                    raise ValueError
                return int(val)                 
            elif _type == "float":
                return float(val)
            elif _type == "bool":
                if bool(val) != int(val):
                    raise ValueError
                return bool(val)
            elif _type == "dict":
                return dict(val)
            return str(val)


        try:
            if value == "" and _type == "str":
                return value
            if value.startswith('$'):
                val = os.getenv(value[1:])
            else:
                val = eval(value, None, user_ns)

            # check value if of the right type
            try:
                return _convert(val, _type)
            except:
                return _convert(eval(val), _type)
        except:
            raise ValueError("failed to set {0}, due to invalid {1} value {2}.".format(key, _type, value))
