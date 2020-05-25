# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""A module that contains general purpose util functions.
"""

import re
import os
import json
from decimal import Decimal
import datetime


import isodate


from .constants import Constants



#
# From https://github.com/django/django/blob/master/django/utils/text.py
#
def get_valid_name(name: str) -> str:
    """
    Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    """
    # name = str(name).strip().replace(' ', '_')
    name = str(name).strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', name)


def get_valid_filename_with_spaces(name: str) -> str:
    """
    Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    """
    # name = str(name).strip().replace(' ', '_')
    name = str(name).strip()
    return re.sub(r'(?u)[^-\w. ]', '', name)



# Expression to match some_token and some_token="with spaces" (and similarly
# for single-quoted strings).
smart_split_re = re.compile(r"""
    ((?:
        [^\s\n\r\f\t'"]*
        (?:
            (?:"(?:[^"\\]|\\.)*" | '(?:[^'\\]|\\.)*')
            [^\s\n\r\f\t'"]*
        )+
    ) | \S+)
""", re.VERBOSE)


smart_split_lines_re = re.compile(r"""
    ((?:
        [^\s\n\r\f\t'"]*
        (?:
            (?:"(?:[^"\\]|\\.)*" | '(?:[^'\\]|\\.)*')
            [^\s\n\r\f\t'"]*
        )+
    ) | \S+)
""", re.VERBOSE)


def smart_split(text):
    r"""
    Generator that splits a string by spaces, leaving quoted phrases together.
    Supports both single and double quotes, and supports escaping quotes with
    backslashes. In the output, strings will keep their initial and trailing
    quote marks and escaped quotes will remain escaped (the results can then
    be further processed with unescape_string_literal()).
    >>> list(smart_split(r'This is "a person\'s" test.'))
    ['This', 'is', '"a person\\\'s"', 'test.']
    >>> list(smart_split(r"Another 'person\'s' test."))
    ['Another', "'person\\'s'", 'test.']
    >>> list(smart_split(r'A "\"funky\" style" test.'))
    ['A', '"\\"funky\\" style"', 'test.']
    """
    for bit in smart_split_re.finditer(str(text)):
        yield bit.group(0)


#
# my
# 
def split_lex(text: str):
    return list(smart_split(text))


def convert_to_common_path_obj(_path: str):
    prefix = ""
    path = _path.replace("\\", "/")
    if path.startswith("file:"):
        path = path[5:]
        if path.startswith("///"):
            path = path[3:]
        elif path.startswith("//"):
            pass
        elif path.startswith("/"):
            path = path[1:]

    parts = path.split(":")
    if len(parts) > 1:
        prefix = parts[0] + ":"
        path = ":".join(parts[1:])
        if path.startswith("//"):
            prefix += "//"
            path = path[2:]
    elif path.startswith("//"):
        prefix = "//"
        path = path[2:]
        
    parts = path.split("/")
    # parts = [get_valid_name(part) for part in parts] if not allow_spaces else [get_valid_filename_with_spaces(part) for part in parts]
    parts = [get_valid_filename_with_spaces(part) for part in parts]
    path = "/".join(parts)
    return {"prefix": prefix, "path": path}


def adjust_path_to_uri(_path: str) -> str:
    path_obj = convert_to_common_path_obj(_path)
    return path_obj.get("prefix") + path_obj.get("path")


def adjust_path(_path: str) -> str:
    path = adjust_path_to_uri(_path)
    path = os.path.normpath(path)
    return path


def safe_str(s) -> str:
    try:
        return f"{s}"
    except:
        return "<failed safe_str()>"


def quote_spaced_items_in_path(_path: str) -> str:
    path = _path.replace("\\", "/")
    items = path.split("/")
    for idx, item in enumerate(items):
        if item.find(" ") >= 0:
            items[idx] = f'"{item}"'
    path = "/".join(items)
    # path = os.path.normpath(path)
    return path


def json_defaults(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, datetime.timedelta):
        return timedelta_to_timespan(obj, minimal=True)
        # return isodate.duration_isoformat(obj)
        # return (datetime.datetime.min + obj).time().isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    raise TypeError


def json_dumps(_dict:dict, **kwargs)->str:
    return json.dumps(_dict, default=json_defaults, **kwargs)


def timedelta_to_timespan(_timedelta:datetime.timedelta, minimal:bool=None)->str:
    total_seconds = _timedelta.total_seconds()
    days = total_seconds // Constants.DAY_SECS
    rest_secs = total_seconds - (days * Constants.DAY_SECS)

    hours = rest_secs // Constants.HOUR_SECS
    rest_secs = rest_secs - (hours * Constants.HOUR_SECS)

    minutes = rest_secs // Constants.MINUTE_SECS
    rest_secs = rest_secs - (minutes * Constants.MINUTE_SECS)

    seconds = rest_secs // 1
    rest_secs = rest_secs - seconds

    ticks = rest_secs * Constants.TICK_TO_INT_FACTOR
    if minimal == True:
        result = "{0:02}:{1:02}:{2:02}".format(int(hours), int(minutes), int(seconds))
        if days > 0:
            result = "{0:01}.{1}".format(int(days), result)
        if ticks > 0:
            result = "{0}.{1:07}",format(result, int(ticks))
    else:
        result = "{0:01}.{1:02}:{2:02}:{3:02}.{4:07}".format(int(days), int(hours), int(minutes), int(seconds), int(ticks))
    return result
