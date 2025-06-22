# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from datetime import timedelta, datetime
from decimal import Decimal


from .dependencies import Dependencies
from .my_utils import json_dumps, timedelta_to_timespan


pandas = Dependencies.get_module("pandas", dont_throw=True)
if pandas:
    DataFrame = pandas.DataFrame
    Series = pandas.Series
else:
    class DataFrame(object):
        pass
    class Series(object):
        pass

class CurlyBracketsParamsDict(dict):
    
    def __init__(self, parameter_vars, override_vars):
        super(CurlyBracketsParamsDict, self).__init__()
        self._used_params_dict = {}
        self._eval_params_dict = {}
        self._parameter_vars = parameter_vars or {}
        self._override_vars = override_vars or {}
        self._eval_off = True


    def __getitem__(self, key):

        if key in self._override_vars:
            value = self._override_vars[key]
            if self._eval_off:
                self._used_params_dict[key] = value
            else:
                self._eval_params_dict[key] = value
        elif key in self._parameter_vars:
            value = self._parameter_vars[key]
            if self._eval_off:
                self._used_params_dict[key] = value
            else:
                self._eval_params_dict[key] = value
        elif self._eval_off:
            try:
                self._eval_off = False
                self._eval_params_dict = {}
                # eval may cause it to become recursive, because it uses self as the namespace
                value = eval(key, self)
                for k in self._eval_params_dict:
                    self._used_params_dict[k] = self._eval_params_dict[k]
            except: # pylint: disable=bare-except
                
                value = f'{{{key}}}'
            finally:
                self._eval_off = True
        else:
            value = super(CurlyBracketsParamsDict, self).__getitem__(key)

        if self._eval_off:
            if type(value) == str:
                pass
            else:
                value = Parameterizer._object_to_kql(value)
        return value


    @property 
    def used_params_dict(self):
        return self._used_params_dict


class Parameterizer(object):
    """parametrize query by prefixing query with kql let statements, that resolve query unresolved let statements"""

    def __init__(self, query: str):
        self._query = query
        self._parameters = None
        self._statements =None


    def apply(self, parameter_vars:dict, override_vars:dict=None, **options):
        """expand query to include resolution of python parameters"""
        override_vars = override_vars if type(override_vars) == dict else {}

        if options.get("enable_curly_brackets_params"):
            curly_brackets_params_dict = CurlyBracketsParamsDict(parameter_vars, override_vars)
            curly_brackets_parametrized_query = self._query.format_map(curly_brackets_params_dict)
            used_params_dict = curly_brackets_params_dict.used_params_dict
        else:
            curly_brackets_parametrized_query = self._query
            used_params_dict = {}

        self._query_management_prefix = ""
        self._query_body = curly_brackets_parametrized_query
        if self._query_body.startswith("."):
            parts = self._query_body.split("<|")  
            if len(parts) == 2:
                self._query_management_prefix = parts[0] + "<| "
                self._query_body = parts[1].strip()
        q = self._normalize(self._query_body)
        self._query_let_statments = [s.strip()[4:].strip() for s in q.split(";") if s.strip().startswith("let ")]

        parameter_keys = self._detect_parameters(self._query_let_statments, parameter_vars, override_vars)
        self._parameters = self._set_parameters(parameter_keys, parameter_vars, override_vars)
        self._statements = self._build_let_statements(self._parameters)
        self._statements.append(self._query_body)
        self._parameters.update(used_params_dict)
        return self

    @property 
    def parameters(self):
        return self._parameters


    @property 
    def query(self):
        return self._query if self._parameters is None else self._query_management_prefix + ";".join(self._statements)


    @property
    def pretty_query(self):
        if self._parameters is None:
            return self._query
        else:
            query_management_prefix = self._query_management_prefix or ''
            if (query_management_prefix and len(query_management_prefix) > 0):
                query_management_prefix += '\n'
            return query_management_prefix  + ";\n".join(self._statements)


    @classmethod
    def _object_to_kql(cls, v)->str:
        try:
            val = (
                repr(v)
                if isinstance(v, str)
                else "null"
                if v is None
                else str(v).lower()
                if isinstance(v, bool)
                else cls._timedelta_to_timespan(v)
                if isinstance(v, timedelta)
                else f"datetime({v.isoformat()})"  # kql will assume utc time
                if isinstance(v, datetime)
                else f"dynamic({json_dumps(dict(v))})"
                if isinstance(v, dict)
                else f"dynamic({json_dumps(list(v))})"
                if isinstance(v, (list, Series))
                else f"dynamic({json_dumps(list(tuple(v)))})"
                if isinstance(v, tuple)
                else f"dynamic({json_dumps(list(set(v)))})"
                if isinstance(v, set)
                else cls._datatable(v)
                if isinstance(v, DataFrame)
                else "datetime(null)"
                if str(v) == "NaT"
                else "time(null)"
                if str(v) == "nat"  # does not exist
                else "real(null)"
                if str(v) == "nan"  # missing na for long(null)
                else f"'{v.decode('utf-8')}'"
                if isinstance(v, bytes)
                else f"long({v})"
                if isinstance(v, int)
                else f"real({v})"
                if isinstance(v, float)
                else f"decimal({v})"
                if isinstance(v, Decimal)
                else str(v)
            )
        except: # pylint: disable=bare-except
            val = f"'{v}'"
        return str(val)


    def _build_let_statements(self, parameters: dict)-> list:
        """build let statements that resolve python variable names to python variables values"""
        statements = []
        for k in parameters:
            v = parameters.get(k)
            # print('type', type(v))
            val = self._object_to_kql(v)
            statements.append(f"let {k} = {val}")
        return statements


    def _set_parameters(self, parameter_keys: list, parameter_vars: dict, override_vars: dict) -> dict:
        """set values for the parameters"""
        parameters = {}
        for k in parameter_keys:
            if k in override_vars:
                v = override_vars[k]

            elif k in parameter_vars:
                v = parameter_vars[k]

            else:
                continue
            
            parameters[k] = v
        return parameters
    

    _DATAFRAME_TO_KQL_TYPES = {
        "int8": "long",
        "int16": "long",
        "int32": "long",
        "int64": "long",
        "uint8": "long",
        "uint16": "long",
        "uint32": "long",
        "uint64": "long",
        "float16": "real",
        "float32": "real",
        "float64": "real",
        "complex64": "dynamic",
        "complex128": "dynamic",
        "character": "string",
        "bytes": "string",
        "str": "string",
        "void": "string",
        "record": "dynamic",
        "bool": "bool",
        "datetime": "datetime",
        "datetime64": "datetime",
        "object": None,
        "category": "string",
        "timedelta": "timespan",
        "timedelta64": "timespan",
    }
 
    @classmethod
    def _dataframe_to_kql_value(cls, val, pair_type:list) -> str:
        pd_type, kql_type = pair_type
        s = str(val)
        if kql_type == "string":
            if pd_type == "bytes":
                s = val.decode("utf-8")
            return "" if s is None else f"'{s}'"
        if kql_type == "long": 
            return 'long(null)' if s == 'nan' else f"long({s})"
        if kql_type == "real": 
            return 'real(null)' if s == 'nan' else f"real({s})"
        if kql_type == "bool": 
            return 'true' if val is True else 'false' if val is False else 'bool(null)'
        if kql_type == "datetime":
            return 'datetime(null)' if s == "NaT" else f"datetime({s})"  # assume utc
        if kql_type == "timespan":
            return 'time(null)' if s == "NaT" else cls._timedelta_to_timespan(val)
        if kql_type == "dynamic":
            if pd_type == "record":
                return cls._object_to_kql(list(val))
            elif pd_type in ["complex64", "complex128"]:
                return cls._object_to_kql([val.real, val.imag])
            else:
                return cls._object_to_kql(val)
         
        # this is the best we not specified
        return "" if s is None else repr(s)

    @classmethod
    def _guess_object_types(cls, pairs_type:dict, r:list) -> dict:
        new_pairs_type = {}
        for idx, col in enumerate(pairs_type):
            pair = pairs_type[col]
            if pair[0] == "object":
                ty = None
                for row in r:
                    val = row[idx]
                    if val is not None:
                        cty = type(val)
                        if cty == str or str(val) not in ["nan", "NaT"]:
                            ty = ty or cty
                            if ty != cty or ty == str:
                                ty = str
                                break
                            if ty in [dict, list, set, tuple]:
                                try:
                                    json_dumps(val)
                                except: # pylint: disable=bare-except
                                    ty = str
                                    break
                if ty == str:
                    new_pairs_type[col] = [pair[0], "string"]
                elif ty == bool:
                    new_pairs_type[col] = [pair[0], "bool"]
                elif ty in [dict, list, set, tuple]:
                    new_pairs_type[col] = [pair[0], "dynamic"]
                elif ty == int:
                    new_pairs_type[col] = [pair[0], "long"]
                elif ty == float:
                    new_pairs_type[col] = [pair[0], "real"]
                elif str(ty).split(".")[-1].startswith("datetime"):
                    new_pairs_type[col] = [pair[0], "datetime"]
                elif str(ty).split(".")[-1].startswith("timedelta"):
                    new_pairs_type[col] = [pair[0], "timespan"]
                elif ty == bytes:
                    new_pairs_type[col] = ["bytes", "string"]
                else:
                    new_pairs_type[col] = [pair[0], "string"]
            else:
                new_pairs_type[col] = pair
        return new_pairs_type

    @classmethod
    def _datatable(cls, df:DataFrame) -> str:
        t = {col: str(t).split(".")[-1].split("[",1)[0] for col, t in dict(df.dtypes).items()}
        d = df.to_dict("split")
        # i = d["index"]
        c = d["columns"]
        r = d["data"]
        pairs_t = {col: [str(t[col]), cls._DATAFRAME_TO_KQL_TYPES.get(str(t[col]))] for col in c}
        pairs_t = cls._guess_object_types(pairs_t, r)
        schema = ", ".join([f'["{str.strip(col)}"]:{pairs_t[col][1]}' for col in c])
        data = ", ".join([", ".join([cls._dataframe_to_kql_value(val, pairs_t[c[idx]]) for idx, val in enumerate(row)]) for row in r])
        return f" view () {{datatable ({schema}) [{data}]}}"      
 

    @classmethod
    def _detect_parameters(cls, query_let_statments: list, parameter_vars: dict, override_vars: dict)-> list:
        """detect in query let staements, the unresolved parameter that can be resolved by python variables"""
        set_keys = []
        parameters = []
        for statment in query_let_statments:
            kv = statment.split("=")
            if len(kv) == 2: 
                param_name = kv[1].strip()
                if (not param_name.startswith(('"', "'"))
                        and "(" not in param_name
                        and "[" not in param_name
                        and "{" not in param_name
                        and param_name[0] not in [str(i) for i in range(10)]
                        and param_name != "true"
                        and param_name != "false"
                        and param_name not in set_keys
                        and (param_name in parameter_vars or param_name in override_vars)):

                    parameters.append(param_name)
            key = kv[0].strip()
            set_keys.append(key)
        return parameters


    def _normalize(self, query: str):
        """convert query to one line without comments"""
        lines = []
        for line in query.split("\n"):
            idx = line.find("//")
            if idx >= 0:
                lines.append(line[:idx])
            else:
                lines.append(line)
        return " ".join([line.replace("\r", "").replace("\t", " ") for line in lines])
    
    @classmethod
    def _timedelta_to_timespan(cls, _timedelta:timedelta):

        return f"time({timedelta_to_timespan(_timedelta)})"
