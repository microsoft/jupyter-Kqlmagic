#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

""" Version file. """
VERSION = "0.1.55"






import requests

def get_pypi_latest_version(package_name):
    query_endpoint = 'https://pypi.org/pypi/{0}/json'.format(package_name)
    response = requests.get(query_endpoint)
    if response.status_code == 200:
        json_response = response.json()
        return json_response["info"]["version"]

def compare_version(other):

    def is_int(val):
        for c in val:
            if c not in '0123456789': return False
        return len(val) > 0

    v = VERSION.strip('.')
    o = other.strip('.')
    for idx, v_val in enumerate(v):
        if idx >= len(o):
            if v_val != '0': return -1
        o_val = o[idx]
        if is_int(o_val) and is_int(v_val):
            o_num = int(o_val)
            v_num = int(v_val)
            if v_num > o_num : return -1
            if v_num < o_num : return 1
        elif is_int(o_val): return 1
        elif is_int(v_val): return -1
        elif v_val > o_val: return -1
        elif v_val < o_val: return 1
    return 0

