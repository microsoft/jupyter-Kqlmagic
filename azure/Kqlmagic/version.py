#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

#
#  _  __          _                               _        
# | |/ /   __ _  | |  _ __ ___     __ _    __ _  (_)   ___ 
# | ' /   / _` | | | | '_ ` _ \   / _` |  / _` | | |  / __|
# | . \  | (_| | | | | | | | | | | (_| | | (_| | | | | (__ 
# |_|\_\  \__, | |_| |_| |_| |_|  \__,_|  \__, | |_|  \___|
#            |_|                          |___/            
# binary:
#
# 01001011 01110001 01101100 01101101 01100001 01100111 01101001 01100011 
#
# morse:
#
# -.- --.- .-.. -- .- --. .. -.-. 
# 
# towpoint:
#
# |/(||._ _  _ (~|o _
# |\ ||| | |(_| _||(_
#
# italic
#
#  /__/  _  /  _   _   _  '  _ 
# /  )  (/ (  //) (/  (/ /  (  
#       /            _/     
#
#
# goofy:
#
# \   | )  /  /    | \   |    |        |    /  \      )  ____)  (_    _)  /  __) 
#  |  |/  /  (     |  |  |    |  |\/|  |   /    \    /  /  __     |  |   |  /    
#  |     (    \__  |  |  |    |  |  |  |  /  ()  \  (  (  (  \    |  |   | |     
#  |  |\  \      | |  |  |__  |  |  |  | |   __   |  \  \__)  )  _|  |_  |  \__  
# /   |_)  \_____| |_/      )_|  |__|  |_|  (__)  |___)      (__(      )__\    )_
#
# efiwater
#
#  _       _                  o     
#  )L7 __  )) _  _  ___  ___  _  __ 
# ((`\((_)(( ((`1( ((_( ((_( (( ((_ 
#       ))                _))      
# 
# rev:
#
# ========================================================
# =  ====  =========  ====================================
# =  ===  ==========  ====================================
# =  ==  ===========  ====================================
# =  =  ======    ==  ==  =  = ====   ====   ===  ===   ==
# =     =====  =  ==  ==        ==  =  ==  =  ======  =  =
# =  ==  ====  =  ==  ==  =  =  =====  ===    ==  ==  ====
# =  ===  ====    ==  ==  =  =  ===    =====  ==  ==  ====
# =  ====  =====  ==  ==  =  =  ==  =  ==  =  ==  ==  =  =
# =  ====  =====  ==  ==  =  =  ===    ===   ===  ===   ==
# ========================================================
#

                                                                                                                                          
                                                                                                                                          
# KKKKKKKKK    KKKKKKK                    lllllll                                                                 iiii                      
# K:::::::K    K:::::K                    l:::::l                                                                i::::i                     
# K:::::::K    K:::::K                    l:::::l                                                                 iiii                      
# K:::::::K   K::::::K                    l:::::l                                                                                           
# KK::::::K  K:::::KKK   qqqqqqqqq   qqqqq l::::l    mmmmmmm    mmmmmmm     aaaaaaaaaaaaa      ggggggggg   gggggiiiiiii     cccccccccccccccc
#   K:::::K K:::::K     q:::::::::qqq::::q l::::l  mm:::::::m  m:::::::mm   a::::::::::::a    g:::::::::ggg::::gi:::::i   cc:::::::::::::::c
#   K::::::K:::::K     q:::::::::::::::::q l::::l m::::::::::mm::::::::::m  aaaaaaaaa:::::a  g:::::::::::::::::g i::::i  c:::::::::::::::::c
#   K:::::::::::K     q::::::qqqqq::::::qq l::::l m::::::::::::::::::::::m           a::::a g::::::ggggg::::::gg i::::i c:::::::cccccc:::::c
#   K:::::::::::K     q:::::q     q:::::q  l::::l m:::::mmm::::::mmm:::::m    aaaaaaa:::::a g:::::g     g:::::g  i::::i c::::::c     ccccccc
#   K::::::K:::::K    q:::::q     q:::::q  l::::l m::::m   m::::m   m::::m  aa::::::::::::a g:::::g     g:::::g  i::::i c:::::c             
#   K:::::K K:::::K   q:::::q     q:::::q  l::::l m::::m   m::::m   m::::m a::::aaaa::::::a g:::::g     g:::::g  i::::i c:::::c             
# KK::::::K  K:::::KKKq::::::q    q:::::q  l::::l m::::m   m::::m   m::::ma::::a    a:::::a g::::::g    g:::::g  i::::i c::::::c     ccccccc
# K:::::::K   K::::::Kq:::::::qqqqq:::::q l::::::lm::::m   m::::m   m::::ma::::a    a:::::a g:::::::ggggg:::::g i::::::ic:::::::cccccc:::::c
# K:::::::K    K:::::K q::::::::::::::::q l::::::lm::::m   m::::m   m::::ma:::::aaaa::::::a  g::::::::::::::::g i::::::i c:::::::::::::::::c
# K:::::::K    K:::::K  qq::::::::::::::q l::::::lm::::m   m::::m   m::::m a::::::::::aa:::a  gg::::::::::::::g i::::::i  cc:::::::::::::::c
# KKKKKKKKK    KKKKKKK    qqqqqqqq::::::q llllllllmmmmmm   mmmmmm   mmmmmm  aaaaaaaaaa  aaaa    gggggggg::::::g iiiiiiii    cccccccccccccccc
#                                 q:::::q                                                               g:::::g                             
#                                 q:::::q                                                   gggggg      g:::::g                             
#                                q:::::::q                                                  g:::::gg   gg:::::g                             
#                                q:::::::q                                                   g::::::ggg:::::::g                             
#                                q:::::::q                                                    gg:::::::::::::g                              
#                                qqqqqqqqq                                                      ggg::::::ggg                                
#                                                                                                  gggggg                                   

"""A module that manage package version.
"""

import sys


import requests


from .constants import Constants
from .help import MarkdownString


VERSION = "0.1.108.post3"


def execute_version_command() -> MarkdownString:
    """ execute the version command.
    command just return a string with the version that will be displayed to the user

    Returns
    -------
    str
        A string with the current version
    """
    return MarkdownString(f"{Constants.MAGIC_PACKAGE_NAME} version: {VERSION}")


def get_pypi_latest_version(package_name: str, only_stable_version: bool) -> str:
    """ Retreives latest package version string for PyPI.

    Parameters
    ----------
    package_name : str
        Name of package as define in PyPI.

    Returns
    -------
    str or None
        The latest version string of package in PyPI, or None if fails to retrieve

    Raises
    ------
    RequestsException
        If request to PyPI fails.
    """

    #
    # reterive package data
    #

    json_response = _retreive_package_from_pypi(package_name)
    version = json_response.get("info").get("version") if json_response and json_response.get("info") else None

    if only_stable_version and version and not _is_stable_version(version):
        version = _get_latest_stable_version(json_response) or version
    return version


def _is_stable_version(version: str) -> bool:
    v = _normalize_version(version)
    return len([True for p in v if to_int(p) is None and not p.startswith("post")]) == 0


def _get_latest_stable_version(json_response: str) -> bool:
    latest_stable_version = None
    stable_versions = [v for v in json_response['releases'].keys() if _is_stable_version(v)]
    if len(stable_versions)  > 0:
        latest_stable_version = stable_versions[0] 
        for v in stable_versions:
            if compare_version(v, latest_stable_version, False) > 0:
                latest_stable_version = v
    return latest_stable_version


def compare_version(other: str, version: str, ignore_current_version_post: bool) -> int:
    """ Compares current version to another version string.

    Parameters
    ----------
    other : str
        The other version to compare with, assume string "X.Y.Z" X,Y,Z integers
    version : str
        The current version to compare with, assume string "X.Y.Z" X,Y,Z integers
    ignore_current_version_post : bool
        If set the comparison should ignore current version post versions


    Returns
    -------
    int
        -1 if version higher than other
         0 if version equal to other
         1 if version lower than other
    """

    # normalize for comaprison
    v = _normalize_version(version)
    o = _normalize_version(other)
    v_len = _len_version(v)
    o_len = _len_version(o)
    v = _pad_version(v, o_len - v_len)
    o = _pad_version(o, v_len - o_len)
    
    if len(v) > len(o):
        o.append(0)
    elif len(v) < len(o):
        v.append(0)

    for idx, v_val in enumerate(v):
        o_val = o[idx]
        if o_val != v_val:
            v_int = to_int(v_val)
            o_int = to_int(o_val)
            # both are int, so they can be compared, and also be equal ('05' == '5')
            if o_int is not None and v_int is not None:
                if v_int != o_int:
                    return -1 if o_int < v_int else 1
            # both are not int
            elif o_int is None and v_int is None:
                if v_val.startswith("post"):
                    if o_val.startswith("post"):
                        if ignore_current_version_post:
                            return 0
                        o_int = _post_sub_version(o_val)
                        v_int = _post_sub_version(v_val)
                        if v_int != o_int:
                            return -1 if o_int < v_int else 1
                    elif o_val.startswith("dev"):
                        return -1
                    else:
                        return 1
                elif o_val.startswith("post"):
                    if v_val.startswith("dev"):
                        return 1
                    else:
                        return -1
                elif v_val.startswith("dev"):
                    if o_val.startswith("dev"):
                        o_int = _post_sub_version(o_val)
                        v_int = _post_sub_version(v_val)
                        if v_int != o_int:
                            return -1 if o_int < v_int else 1
                    else:
                        return 1
                elif o_val.startswith("dev"):
                    return -1
                else:
                    val = _compare_pre_sub_version(o_val, v_val)
                    if val != 0:
                        return val

            # any value not int is interpreted as less than 0
            elif o_int is None:
                if o_val.startswith("post"):
                    return 0 if ignore_current_version_post else 1
                if o_val.startswith("dev"):
                    return -1
                o_int = _pre_release_sub_version(o_val)
                return -1 if o_int <= v_int else 1
            else:
                if v_val.startswith("post"):
                    return -1
                if v_val.startswith("dev"):
                    return 1
                v_int = _pre_release_sub_version(v_val)
                return -1 if o_int < v_int else 1
    return 0


def _pre_release_sub_version(v: str):
    parts = _pre_sub_version_parts(v)
    return parts[0] or 0

def _compare_pre_sub_version(o: str, v: str):
    v_parts = _pre_sub_version_parts(v)
    o_parts = _pre_sub_version_parts(o)
    for idx in range(3):
        if v_parts[idx] != o_parts[idx]:
            return 1 if o_parts[idx] > v_parts[idx] else -1
    return 0


def _pre_sub_version_parts(v: str):
    for delim in "abc":
        parts = v.split(delim, 1)
        if len(parts) == 2:
            return [to_int(parts[0]) or 0, delim, to_int(parts[1]) or 0]
    return [0, " ", 0]

def _post_sub_version(v: str):
    parts = v.split("post", 1)
    return to_int(parts[1]) or 0


def _dev_sub_version(v: str):
    parts = v.split("dev", 1)
    return to_int(parts[1]) or 0


def _normalize_version(v: str) -> list:

    # case in-sensitive, and remove trailing whitespaces
    v = v.strip().lower()

    # if starts with "v" remove it
    v = v[1:] if v.startswith("v") else v

    # handle epoch prefix, if doesn't exist assume 0
    v = v.replace("!", ".") if len(v.split("!", 1)) == 2 else f"0.{v}"

    # if post or dev use "-" or "_" instead of "." replace
    v = v.replace("-post",".post").replace("_post",".post").replace("-dev",".dev").replace("_dev",".dev")

    # ignore all the other "-" or "_" delimiters
    v = v.replace("-", "").replace("_", "")

    # replace the exploct pre version to "a", "b", "c" characters
    v = v.replace("alpha", "a").replace("beta", "b").replace("pre", "c").replace("rc", "c")

    # if pre-release characters after "." remove "."
    v = v.replace(".a", "0a").replace(".b", "0b").replace(".c", "0c")

    return v.split(".")


def _len_version(v_list: list) -> int:
    """ Compute length of the component, but without the last component if it is a dev or post"""
    l = len(v_list)
    return l - 1 if v_list[-1].startswith("dev") or v_list[-1].startswith("post") else l


def _pad_version(v_list: list, l: int) -> list:
    """ pad the list at the end with 0 value, but if last component is dev or post, the padding is before the last"""
    if l > 0:
        last = None
        if  v_list[-1].startswith("dev") or v_list[-1].startswith("post"):
            last = v_list[-1]
            v_list = v_list[:-1]
        for idx in range(l):
            v_list.append(0)
        if last:
            v_list.append(last)
    return v_list


def _retreive_package_from_pypi(package_name: str) -> dict:
    """ Retreives package data from PyPI.

    Parameters
    ----------
    package_name : str
        Name of package as define in PyPI.

    Returns
    -------
    dict or None
        The package PyPI data, or None if fails to retrieve

    Raises
    ------
    RequestsException
        If request to PyPI fails.
    """

    api_url = f"https://pypi.org/pypi/{package_name}/json"
    response = requests.get(api_url)

    #
    # handle response
    #

    response.raise_for_status()
    json_response = response.json()
    return json_response


def is_int(str_val: str) -> bool:
    """ Checks whether a string can be converted to int.

    Parameters
    ----------
    str_val : str
        A string to be checked.

    Returns
    -------
    bool
        True if can be converted to int, otherwise False
    """

    return not (len(str_val) == 0 or any([c not in "0123456789" for c in str_val]))


def to_int(str_val: str) -> int:
    """ Converts string to int if possible.
    
    Parameters
    ----------
    str_val : str
        A string to be converted.

    Returns
    -------
    int or None
        Converted integer if success, otherwise None
    """

    return int(str_val) if is_int(str_val) else None


def validate_required_python_version_running(minimal_required_version: str) -> None:
    """ Validate whether the running python version meets minimal required python version 
    
    Parameters
    ----------
    minimal_required_version : str
        Minimal required python version, in the following format: major.minor.micro

    Returns
    -------
    None

    Exceptions
    ----------
    Raise RunTime exception, if sys.version_info does not support attributes: major, minor, micro (old python versions)
    Raise RunTime exception, if running python version is lower than required python version 

    """
    try:
        parts = minimal_required_version.split(".")
        min_py_version = 1000000*int(parts[0]) + 1000*(int(parts[1]) if len(parts) > 1 else 0) + (int(parts[2]) if len(parts) > 2 else 0)
        running_py_version = 1000000*sys.version_info.major + 1000*sys.version_info.minor + sys.version_info.micro
        if running_py_version < min_py_version:
            raise RuntimeError("")
    except:
        raise RuntimeError(f"Kqlmagic requires python >= {Constants.MINIMAL_PYTHON_VERSION_REQUIRED}, you use python {sys.version}")

