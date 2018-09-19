from os.path import expandvars
import six
from six.moves import configparser as CP
from kql.log import Logger, logger
from traitlets import Bool, Int, Unicode, Enum, Float, TraitError


class Parser(object):
    @staticmethod
    def parse(cell, config):
        """Separate input into (connection info, KQL statements, options)"""

        parsed_queries = []
        # split to max 2 parts. First part, parts[0], is the first string.
        parts = [part.strip() for part in cell.split(None, 1)]
        # print(parts)
        if not parts:
            parsed_queries.append({"connection": "", "kql": "", "options": {}})
            return parsed_queries

        #
        # replace substring of the form $name or ${name}, in windows also %name% if found in env variabes
        #
        parts[0] = expandvars(parts[0])  # for environment variables

        # assume connection is specified
        connection = parts[0]
        code = parts[1] if len(parts) > 1 else ""

        #
        # connection taken from a section in  dsn file (file name have to be define in config.dsn_filename or specified as a parameter)
        #
        if parts[0].startswith("[") and parts[0].endswith("]"):
            section = parts[0].lstrip("[").rstrip("]")
            parser = CP.ConfigParser()

            # parse to get flag, for the case that the file nema is specified
            kql, options = Parser._parse_kql_options(code, config)
            # print( "filename: {}".format(options.get("dsn_filename")))
            # with open(options.get("dsn_filename"), "r") as text_file:
            #     print ("file.content: {} ".format(text_file.read()))

            parser.read(options.get("dsn_filename", config.dsn_filename))
            cfg_dict = dict(parser.items(section))
            cfg_dict_lower = dict()
            # for k,v in cfg_dict:
            #     cfg_dict_lower[k.lower()] = v
            cfg_dict_lower = {k.lower(): v for (k, v) in cfg_dict.items()}
            if cfg_dict_lower.get("appid"):
                connection_list = []
                for key in ["appid", "appkey"]:
                    if cfg_dict_lower.get(key):
                        connection_list.append(str.format("{0}('{1}')", key, cfg_dict_lower.get(key)))
                connection = "appinsights://" + ".".join(connection_list)
            elif cfg_dict_lower.get("workspace"):
                connection_list = []
                for key in ["workspace", "appkey"]:
                    if cfg_dict_lower.get(key):
                        connection_list.append(str.format("{0}('{1}')", key, cfg_dict_lower.get(key)))
                connection = "loganalytics://" + ".".join(connection_list)
            else:
                if cfg_dict_lower.get("user"):
                    cfg_dict_lower["username"] = cfg_dict_lower.get("user")
                connection_list = []
                for key in ["username", "password", "cluster", "database"]:
                    if cfg_dict_lower.get(key):
                        connection_list.append(str.format("{0}('{1}')", key, cfg_dict_lower.get(key)))
                connection = "kusto://" + ".".join(connection_list)
        #
        # connection not specified, override default
        #
        elif not (
            parts[0].startswith("kusto://") or parts[0].startswith("appinsights://") or 
            parts[0].startswith("loganalytics://") or parts[0].startswith("file://") or "@" in parts[0]
        ):
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
            kql, options = Parser._parse_kql_options(query.strip(), config)
            if suppress_results:
                options["suppress_results"] = True
            parsed_queries.append({"connection": connection.strip(), "kql": kql, "options": options})

        return parsed_queries

    @staticmethod
    def _parse_kql_options(code, config):
        words = code.split()
        options = {}
        options_table = {
            "ad": {"abbreviation": "auto_dataframe"},
            "auto_dataframe": {"flag": "auto_dataframe", "type": "bool", "config": "config.auto_dataframe"},
            "se": {"abbreviation": "short_errors"},
            "short_errors": {"flag": "short_errors", "type": "bool", "config": "config.short_errors"},
            "f": {"abbreviation": "feedback"},
            "feedback": {"flag": "feedback", "type": "bool", "config": "config.feedback"},
            "sci": {"abbreviation": "show_conn_info"},
            "show_conn_info": {"flag": "show_conn_info", "type": "str", "config": "config.show_conn_info"},
            "c2lv": {"abbreviation": "columns_to_local_vars"},
            "columns_to_local_vars": {"flag": "columns_to_local_vars", "type": "bool", "config": "config.columns_to_local_vars"},
            "sqt": {"abbreviation": "show_query_time"},
            "show_query_time": {"flag": "show_query_time", "type": "bool", "config": "config.show_query_time"},
            "esr": {"abbreviation": "enable_suppress_result"},
            "enable_suppress_result": {"flag": "enable_suppress_result", "type": "bool", "config": "config.enable_suppress_result"},
            "pfi": {"abbreviation": "plotly_fs_includejs"},
            "plotly_fs_includejs": {"flag": "plotly_fs_includejs", "type": "bool", "config": "config.plotly_fs_includejs"},
            "pw": {"abbreviation": "popup_window"},
            "popup_window": {"flag": "popup_window", "type": "bool", "init": "False"},
            "al": {"abbreviation": "auto_limit"},
            "auto_limit": {"flag": "auto_limit", "type": "int", "config": "config.auto_limit"},
            "dl": {"abbreviation": "display_limit"},
            "display_limit": {"flag": "display_limit", "type": "int", "config": "config.display_limit"},
            "ptst": {"abbreviation": "prettytable_style"},
            "prettytable_style": {"flag": "prettytable_style", "type": "str", "config": "config.prettytable_style"},
            "var": {"abbreviation": "last_raw_result_var"},
            "last_raw_result_var": {"flag": "last_raw_result_var", "type": "str", "config": "config.last_raw_result_var"},
            "tp": {"abbreviation": "table_package"},
            "table_package": {"flag": "table_package", "type": "str", "config": "config.table_package"},
            "pp": {"abbreviation": "plot_package"},
            "plot_package": {"flag": "plot_package", "type": "str", "config": "config.plot_package"},
            "df": {"abbreviation": "dsn_filename"},
            "dsn_filename": {"flag": "dsn_filename", "type": "str", "config": "config.dsn_filename"},
            "vc": {"abbreviation": "validate_connection_string"},
            "validate_connection_string": {"flag": "validate_connection_string", "type": "bool", "config": "config.validate_connection_string"},
            "aps": {"abbreviation": "auto_popup_schema"},
            "auto_popup_schema": {"flag": "auto_popup_schema", "type": "bool", "config": "config.auto_popup_schema"},
            "jd": {"abbreviation": "json_display"},
            "json_display": {"flag": "json_display", "type": "str", "config": "config.json_display"},
            "ph": {"abbreviation": "popup_help"},
            "popup_help": {"flag": "popup_help", "type": "bool", "init": "False"},
            "ps": {"abbreviation": "popup_schema"},
            "popup_schema": {"flag": "popup_schema", "type": "bool", "init": "False"},
            "pc": {"abbreviation": "palette_colors"},
            "palette_colors": {"flag": "palette_colors", "type": "int", "config": "config.palette_colors"},
            "pd": {"abbreviation": "palette_desaturation"},
            "palette_desaturation": {"flag": "palette_desaturation", "type": "float", "config": "config.palette_desaturation"},
            "pn": {"abbreviation": "palette_name"},
            "palette_name": {"flag": "palette_name", "type": "str", "config": "config.palette_name"},
            "temp_folder_name": {"flag": "temp_folder_name", "readonly": "True", "config": "config.temp_folder_name"},
            "file_schema_folder_name": {"flag": "file_schema_folder_name", "readonly": "True", "config": "config.file_schema_folder_name"},
            "export_folder_name": {"flag": "export_folder_name", "readonly": "True", "config": "config.export_folder_name"},
            "notebook_app": {"flag": "notebook_app", "readonly": "True", "config": "config.notebook_app"},
            "add_kql_ref_to_help": {"flag": "add_kql_ref_to_help", "readonly": "True", "config": "config.add_kql_ref_to_help"},
            "add_schema_to_help": {"flag": "add_schema_to_help", "readonly": "True", "config": "config.add_schema_to_help"},
            "cache" : {"flag": "cache", "readonly": "True", "config": "config.cache"},
            "use_cache" : {"flag": "use_cache", "readonly": "True", "config": "config.use_cache"},
            "version": {"flag": "version", "type": "bool", "init": "False"},
            "palette": {"flag": "palette", "type": "bool", "init": "False"},
            "popup_palettes": {"flag": "popup_palettes", "type": "bool", "init": "False"},
            "pr": {"abbreviation": "palette_reverse"},
            "palette_reverse": {"flag": "palette_reverse", "type": "bool", "init": "False"},
        }

        for value in options_table.values():
            if value.get("config"):
                options[value.get("flag")] = eval(value.get("config"))
            elif value.get("init"):
                options[value.get("flag")] = eval(value.get("init"))

        int_options = {}
        str_options = {}

        if not words:
            return ("", options)
        num_words = len(words)
        trimmed_kql = code
        first_word = 0

        if num_words - first_word >= 2 and words[first_word + 1] == "<<":
            options["result_var"] = words[first_word]
            trimmed_kql = trimmed_kql[trimmed_kql.find("<<") + 2 :]
            first_word += 2

        state = "bool"
        for word in words[first_word:]:
            if state == "bool":
                if not word[0].startswith("-"):
                    break
                word = word[1:]
                trimmed_kql = trimmed_kql[trimmed_kql.find("-") + 1 :]
                bool_value = True
                if word[0].startswith("!"):
                    bool_value = False
                    word = word[1:]
                    trimmed_kql = trimmed_kql[trimmed_kql.find("!") + 1 :]
                if word in options_table.keys():
                    obj = options_table.get(word)
                    if obj.get("readonly"):
                        raise ValueError("option {0} is readony, cannot be set".format(word))
                    if obj.get("abbreviation"):
                        obj = options_table.get(obj.get("abbreviation"))
                    type = obj.get("type")
                    key = obj.get("flag")
                    option_config = obj.get("config")
                    if type == "bool":
                        options[key] = bool_value
                        trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word) :]
                    state = type
                else:
                    raise ValueError("unknown option")
            elif state == "int":
                if not bool_value:
                    raise ValueError("option {0} cannot be negated".format(word))
                trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word) :]
                options[key] = int(word)
                state = "bool"
            elif state == "float":
                if not bool_value:
                    raise ValueError("option {0} cannot be negated".format(word))
                trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word) :]
                options[key] = float(word)
                state = "bool"
            elif state == "str":
                trimmed_kql = trimmed_kql[trimmed_kql.find(word) + len(word) :]
                if not bool_value:
                    word = "!" + word
                options[key] = word
                state = "bool"
            first_word += 1

            # validate using config traits
            if state == "bool" and option_config is not None:
                template = "'{0}'" if type == "str" else "{0}"
                saved = eval(option_config)
                exec(option_config + "=" + template.format(str(options[key]).replace("'", "\\'")))
                exec(option_config + "=" + template.format(str(saved).replace("'", "\\'")))

        if state != "bool":
            raise ValueError("bad options syntax")

        if num_words - first_word > 0:
            last_word = words[-1].strip()
            if last_word.endswith(";"):
                options["suppress_results"] = True
                trimmed_kql = trimmed_kql[: trimmed_kql.rfind(";")]
        return (trimmed_kql.strip(), options)
