#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import six
from datetime import timedelta, datetime



class Parameterizer(object):
    """parametrize query by prefixing query with kql let statements, that resolve query unresolved let statements"""

    def __init__(self, ns_vars):
        self.ns_vars = ns_vars

    def expand(self, query : str, **kwargs):
        """expand query to include resolution of python parameters"""
        q = self._normalize(query)
        query_let_statments = [s.strip()[4:].strip() for s in q.split(';') if s.strip().startswith('let ')]
        parameters = self._detect_parameters(query_let_statments)
        statements = self._build_let_statements(parameters)
        statements.append(query)
        return ';'.join(statements)

    def _build_let_statements(self, parameters: list):
        """build let statements that resolve python variable names to python variables values"""
        statements = []
        for k in parameters:
            if k in self.ns_vars:
                v = self.ns_vars[k]
                # print('type', type(v))
                val = ("'{0}'".format(v) if isinstance(v, str) 
                                    else 'null' if v is None  
                                    else str(v).lower() if isinstance(v, bool)
                                    else self._timedelta_to_timespan(v) if isinstance(v, timedelta)
                                    else "datetime({0})".format(v) if isinstance(v, datetime) 
                                    else "dynamic({0})".format(dict(v)) if isinstance(v, dict) 
                                    else "dynamic({0})".format(list(v)) if isinstance(v, list) 
                                    else "dynamic({0})".format(tuple(v)) if isinstance(v, tuple) 
                                    else v)
                statements.append('let {0} = {1}'.format(k, val))
        return statements

    def _detect_parameters(self, query_let_statments : list):
        """detect in query let staements, the unresolved parameter that can be resolved by python variables"""
        set_keys = []
        parameters = [] 
        for statment in query_let_statments:
            kv = statment.split('=')
            if len(kv) == 2:
                param_name = kv[1].strip()
                if (
                    not param_name.startswith('"') and not param_name.startswith("'") and
                    not '(' in param_name and not '[' in param_name and not '{' in param_name and
                    not param_name[0] in [str(i) for i in range(10)] and
                    not param_name == 'true' and not param_name == 'false' and
                    not param_name in set_keys and
                    param_name in self.ns_vars):
                    parameters.append(param_name)
            key = kv[0].strip()
            set_keys.append(key)
        return parameters

    def _normalize(self, query : str):
        """convert query to one line without comments"""
        lines=[]
        for line in query.split('\n'):
            idx = line.find('//')
            if idx >= 0:
                lines.append(line[:idx])
            else:
                lines.append(line)
        return ' '.join([line.replace('\r', '').replace('\t', ' ') for line in lines])

    def _timedelta_to_timespan(self, td):
        days = td.days
        rest_secs = td.seconds
        hours = rest_secs // 3600
        rest_secs = rest_secs % 3600
        minutes = rest_secs // 60
        rest_secs = rest_secs % 60
        seconds = rest_secs
        miliseconds = td.microseconds // 100000
        return 'time({0:01}.{1:02}:{2:02}:{3:02}.{4:01})'.format(days, hours, minutes, seconds, miliseconds )
