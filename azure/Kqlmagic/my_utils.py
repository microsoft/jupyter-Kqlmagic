# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""A module that contains general purpose util functions.
"""

import re
import os

#
# From https://github.com/django/django/blob/master/django/utils/text.py
#
def get_valid_filename(name: str) -> str:
    """
    Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    """
    name = str(name).strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', name)


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

def adjust_path_to_uri(_path: str):
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
    parts = [get_valid_filename(part) for part in parts]
    path = "/".join(parts)
    print("DEBUG: adjust_path_to_uri", _path, prefix + path)
    return prefix + path

def adjust_path(_path: str):
    path = adjust_path_to_uri(_path)
    path = os.path.normpath(path)
    print("DEBUG: adjust_path", _path, path)
    return path