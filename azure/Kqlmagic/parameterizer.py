# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import json
from datetime import timedelta, datetime


import six
from pandas import DataFrame, Series


from .constants import Constants


class ExtendedJSONEncoder(json.JSONEncoder):

    def defaultt(self, o):
        if isinstance(o, bytes):
            return o.decode("utf-8")
        else:
            json.JSONEncoder.default(self, o)


class Parameterizer(object):
    """parametrize query by prefixing query with kql let statements, that resolve query unresolved let statements"""

    def __init__(self, ns_vars, override_vars = {}):
        self.ns_vars = ns_vars
        self.override_vars = override_vars


    def expand(self, query: str, **kwargs):
        """expand query to include resolution of python parameters"""
        query_management_prefix = ""
        query_body = query
        if query.startswith("."):
            parts = query.split("<|")  
            if len(parts) == 2:
                query_management_prefix = parts[0] + "<| "
                query_body = parts[1].strip()
        q = self._normalize(query_body)
        query_let_statments = [s.strip()[4:].strip() for s in q.split(";") if s.strip().startswith("let ")]
        parameters = self._detect_parameters(query_let_statments)
        statements = self._build_let_statements(parameters)
        statements.append(query_body)
        return {'parametrized_query': query_management_prefix + ";".join(statements) ,'query_management_prefix':query_management_prefix, 'statements': statements}


    def _object_to_kql(self, v) -> str:
        try:
            val = (
                repr(v)
                if isinstance(v, str)
                else "null"
                if v is None
                else str(v).lower()
                if isinstance(v, bool)
                else self._timedelta_to_timespan(v.total_seconds())
                if isinstance(v, timedelta)
                else f"datetime({v.isoformat()})" # kql will assume utc time
                if isinstance(v, datetime)
                else f"dynamic({json.dumps(dict(v), cls=ExtendedJSONEncoder)})"
                if isinstance(v, dict)
                else f"dynamic({json.dumps(list(v), cls=ExtendedJSONEncoder)})"
                if isinstance(v, (list,Series))
                else f"dynamic({json.dumps(list(tuple(v)) ,cls=ExtendedJSONEncoder)})"
                if isinstance(v, tuple)
                else f"dynamic({json.dumps(list(set(v)), cls=ExtendedJSONEncoder)})"
                if isinstance(v, set)
                else self.datatable(v)
                if isinstance(v, DataFrame)
                else "datetime(null)"
                if str(v) == "NaT"
                else "time(null)"
                if str(v) == "nat" # does not exist
                else "real(null)"
                if str(v) == "nan" # missing na for long(null)
                else f"'{v.decode('utf-8')}'"
                if isinstance(v, bytes)
                else f"long({v})"
                if isinstance(v, int)
                else f"real({v})"
                if isinstance(v, float)
                else str(v)
            )
        except:
            val = f"'{v}'"
        return str(val)


    def _build_let_statements(self, parameters: list):
        """build let statements that resolve python variable names to python variables values"""
        statements = []
        for k in parameters:
            if k in self.override_vars:
                v = self.override_vars[k]

            elif k in self.ns_vars:
                v = self.ns_vars[k]

            else:
                continue

            # print('type', type(v))
            val = self._object_to_kql(v)
            statements.append(f"let {k} = {val}")
        return statements
    

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
 

    def dataframe_to_kql_value(self, val, pair_type:list) -> str:
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
            return 'true' if val == True else 'false' if  val == False else 'bool(null)'
        if kql_type == "datetime":
            return 'datetime(null)' if s == "NaT" else f"datetime({s})" # assume utc
        if kql_type == "timespan":
            return 'time(null)' if s == "NaT" else self._timedelta_to_timespan(val.total_seconds())
        if kql_type == "dynamic":
            if pd_type == "record":
                return self._object_to_kql(list(val))
            elif pd_type in ["complex64", "complex128"]:
                return self._object_to_kql([val.real, val.imag])
            else:
                return self._object_to_kql(val)
         
        # this is the best we not specified
        return "" if s is None else repr(s)

        
    def guess_object_types(self, pairs_type:dict, r:list) -> dict:
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
                                    json.dumps(val, cls=ExtendedJSONEncoder)
                                except:
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


    def datatable(self, df: DataFrame) -> str:
        t = {col: str(t).split(".")[-1].split("[",1)[0] for col, t in dict(df.dtypes).items()}
        d = df.to_dict("split")
        # i = d["index"]
        c = d["columns"]
        r = d["data"]
        pairs_t = {col: [str(t[col]), self._DATAFRAME_TO_KQL_TYPES.get(str(t[col]))] for col in c}
        pairs_t = self.guess_object_types(pairs_t, r)
        schema = ", ".join([f"{col}:{pairs_t[col][1]}" for col in c])
        data = ", ".join([", ".join([self.dataframe_to_kql_value(val, pairs_t[c[idx]]) for idx, val in enumerate(row)]) for row in r])
        return f" view () {{datatable ({schema}) [{data}]}}"      
 

    def _detect_parameters(self, query_let_statments: list):
        """detect in query let staements, the unresolved parameter that can be resolved by python variables"""
        set_keys = []
        parameters = []
        for statment in query_let_statments:
            kv = statment.split("=")
            if len(kv) == 2:
                param_name = kv[1].strip()
                if (
                    not param_name.startswith('"')
                    and not param_name.startswith("'")
                    and not "(" in param_name
                    and not "[" in param_name
                    and not "{" in param_name
                    and not param_name[0] in [str(i) for i in range(10)]
                    and not param_name == "true"
                    and not param_name == "false"
                    and not param_name in set_keys
                    and (param_name in self.ns_vars or param_name in self.override_vars)
                ):
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
    

    def _timedelta_to_timespan(self, total_seconds:float):
        days = total_seconds // Constants.DAY_SECS
        rest_secs = total_seconds - (days * Constants.DAY_SECS)

        hours = rest_secs // Constants.HOUR_SECS
        rest_secs = rest_secs - (hours * Constants.HOUR_SECS)

        minutes = rest_secs // Constants.MINUTE_SECS
        rest_secs = rest_secs - (minutes * Constants.MINUTE_SECS)

        seconds = rest_secs // 1
        rest_secs = rest_secs - seconds

        ticks = rest_secs * Constants.TICK_TO_INT_FACTOR
        return "time({0:01}.{1:02}:{2:02}:{3:02}.{4:07})".format(int(days), int(hours), int(minutes), int(seconds), int(ticks))

