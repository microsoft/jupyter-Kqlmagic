# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""A module that contains general purpose util functions.
"""

import re
import os
import ast
import json
from decimal import Decimal
import datetime
from typing import Any, Union, Generator, List, Dict, Tuple


from .constants import Constants


#
# From https://github.com/django/django/blob/master/django/utils/text.py
#
def get_valid_name(name:str)->str:
    """
    Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    """
    # name = str(name).strip().replace(' ', '_')
    name = str(name).strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', name)


def get_valid_filename_with_spaces(name:str)->str:
    """
    Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    """
    # name = str(name).strip().replace(' ', '_')
    name = str(name).strip()
    return re.sub(r'(?u)[^-\w.@ ]', '', name)


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


def smart_split(text)->Generator[str,None,None]:
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
def split_lex(text:str)->List[str]:
    return list(smart_split(text))


def convert_to_common_path_obj(_path:str)->Dict[str,str]:
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


def adjust_path_to_uri(_path:str)->str:
    path_obj = convert_to_common_path_obj(_path)
    return path_obj.get("prefix") + path_obj.get("path")


def adjust_path(_path:str)->str:
    path = adjust_path_to_uri(_path)
    path = os.path.normpath(path)
    return path


def safe_str(s) -> str:
    try:
        return f"{s}"
    except: # pylint: disable=bare-except
        return "<failed safe_str()>"


def quote_spaced_items_in_path(_path:str)->str:
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
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    else:
        error_message = f"unknown type: {type(obj)}, class name: {obj.__class__.__name__}"
        # this print is not for debug
        print(f'json failed to convert: {error_message}')
    raise TypeError(error_message)


def json_dumps(_dict:Dict[str,Any], **kwargs)->str:
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
    if minimal is True:
        result = "{0:02}:{1:02}:{2:02}".format(int(hours), int(minutes), int(seconds))
        if days > 0:
            result = "{0:01}.{1}".format(int(days), result)
        if ticks > 0:
            result = "{0}.{1:07}".format(result, int(ticks))
    else:
        result = "{0:01}.{1:02}:{2:02}:{3:02}.{4:07}".format(int(days), int(hours), int(minutes), int(seconds), int(ticks))
    return result


def get_env_var(var_name:str, keep_quotes=False)->str:
    value = os.getenv(var_name)
    if value and not keep_quotes:
       _was_quoted, value = strip_if_quoted(value)
    return value


LIST_CHAR_PEER = {
    "(": ")",
    "[": "]",
    "{": "}"
}


def is_collection(string:str, collection_key:str=None)->bool:
    return len(string) > 1 and string[0] in LIST_CHAR_PEER and string[-1] == LIST_CHAR_PEER[string[0]] and string[0] == (collection_key or string[0])


def get_env_var_list(var_name:str, keep_quotes:bool=False)->List[str]:
    # empty strings are ignored
    try:
        items = None
        string = os.getenv(var_name)

        if string is not None:
            _was_quoted, string = (False, string) if keep_quotes else strip_if_quoted(string)
            items = split_if_collection(string, sep=",", strip=True,skip_empty=True)
            if not keep_quotes:
                items = [strip_if_quoted(item)[1] for item in items]

        return items
    except: # pylint: disable=bare-except
        raise SyntaxError(f"failed to parse environment variable {var_name}={os.getenv(var_name)} ")


def get_env_var_bool(var_name:str, default:bool=None)->bool:
    try:
        string = os.getenv(var_name)

        if string:
            _, string  = strip_if_quoted(string)
            string = string.lower()

            if string in ["true", "false"]:
                return string == "true"
        else:
            return default
    except: # pylint: disable=bare-except
        pass
    raise SyntaxError(f"failed to parse environment variable {var_name}={os.getenv(var_name)} ")


def is_env_var(var_name:str)->bool:
    return var_name in os.environ


def split_if_collection(string:str, sep:Union[str,List[str]]=None, strip:bool=None, skip_empty:bool=None)->List[str]:
    if type(string) is str:
        if string and is_collection(string):
            string = string[1:-1].strip()

        items = tokenized_split(string, sep=sep, strip=strip, skip_empty=skip_empty)
        return items
    else:
        raise TypeError("split_if_collection can split only argument of type str")


def strip_if_quoted(string:str)->Tuple[str,bool]:
    was_quoted = False
    try:
        if type(string) is str and len(string) > 1 and string[0] == string[-1] and string[0] in ("'", '"'):
            string = ast.literal_eval(string).strip()
            was_quoted = True
    except: # pylint: disable=bare-except
        pass

    return was_quoted, string


def escape_string(value:Any, quote_char:str)->str:
    import json
    result = json.dumps(f'{value}')[1:-1]
    if (quote_char == "'"
            or (quote_char == "'''" and len(result) > 0 and (result[-1] == "'" or result.startswith("'''")))):
        result = result.replace("'", r"\'")

    if (quote_char != '"'
            and not (quote_char == '"""' and len(result) > 0 and (result[-1] == '"' or result.startswith('"""')))):
        result = result.replace(r'\"', '"')
    return result


def single_quote(*args)->str:
    return _quote(*args, quote_str="'")


def double_quote(*args)->str:
    return _quote(*args, quote_str='"')


def _quote(*args, quote_str='"')->str:
    escaped = "".join([escape_string(arg, quote_str) for arg in args])
    return f"{quote_str}{escaped}{quote_str}"


def tokenized_split(string:str, sep:Union[str,List[str]]=None, strip:bool=None, skip_empty:bool=None)->List[str]:
    # throws if tokenizer can't close properly quotes (ERRORTOKEN)
    # throws when prethesis, brackets or curly parethesis are not balanced/closed (tokenize.TokenError)
    import tokenize

    separators = [sep] if sep is str else sep or [","]
    items = []
    def _append(string:str):
        if strip is True:
            string = string.strip()
        if len(string) > 0 or skip_empty is not True:
            items.append(string)

    start = 0
    collection_start_delimiter = None
    collection_end_delimiter = None
    collection_depth = 0
    for tok in tokenize.generate_tokens(iter([string]).__next__):
        if tok.type == tokenize.ERRORTOKEN:
            if tok.string not in  [' ', '$']:
                raise SyntaxError("ERRORTOKEN")
        elif tok.type == tokenize.ENDMARKER:
            end = len(string)
            sub_string = string[start:end]
            _append(sub_string)
            break
        if collection_depth == 0:
            if tok.type == tokenize.OP and tok.string in separators:
                end = tok.start[1]
                sub_string =string[start:end]
                _append(sub_string)
                start = tok.end[1]
            if tok.type == tokenize.OP and tok.string in LIST_CHAR_PEER:
                collection_start_delimiter = tok.string
                collection_end_delimiter = LIST_CHAR_PEER[collection_start_delimiter]
                collection_depth = 1
        elif tok.type == tokenize.OP and tok.string == collection_start_delimiter:
            collection_depth += 1
        elif tok.type == tokenize.OP and tok.string == collection_end_delimiter:
            collection_depth -= 1

    return items

def get_lines(text:str)->List[str]:

    queries = []
    buffer = []
    inside_triple_quotes = False
    
    lines = text.splitlines(keepends=True)  # Keep line endings

    for line in lines:
        index = line.find('```')
        marker = None
        
        if index != -1:
            count = line.count('```')
            # even number of triple quotes cancel each other as a start or end triple quote
            if count % 2 == 1:
                marker = '```'
        
        if marker:
            if not inside_triple_quotes:
                inside_triple_quotes = True
                buffer.append(line)
            else:
                buffer.append(line)
                queries.append(''.join(buffer))
                buffer.clear()
                inside_triple_quotes = False
        elif inside_triple_quotes:
            buffer.append(line)
        else:
            queries.append(line)

    # Handle unclosed triple-quoted section but not adding the triple quotes by self
    if inside_triple_quotes:
        queries.append(''.join(buffer))
    buffer.clear()

    
    return queries
