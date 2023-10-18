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
import re
from functools import cmp_to_key
from typing import Any, Dict, Tuple, Iterable


from .constants import Constants
from .help import MarkdownString
from ._version import __version__
from .http_client import HttpClient


_IGNORE_POST_VERSION_PATTERN = r'[.]?(post|rev|r)[0-9]*$'
_IS_STABLE_VESRION_PATTERN = r'[v]?([0-9]+[!])?[0-9]+(\.[0-9]*)*([.]?(post|rev|r)[0-9]*)?$'

try:
    import pkg_resources

    def is_stable_version(version:str)->bool:
        parsed_version = pkg_resources.parse_version(version)
        return not parsed_version.is_prerelease  # is_prerelease returns true for .dev or .rc versions


    def compare_version(other:str, version:str, ignore_current_version_post:bool)->int:
        """ Compares current version to another version string.

        Parameters
        ----------
        other : str
            The other version to compare with, assume string "X.Y.Z" X,Y,Z integers
        version : str
            The current version to compare with, assume string "X.Y.Z" X,Y,Z integers
        ignore_current_version_post : bool
            If set the comparison should ignore current version post version information


        Returns
        -------
        int
            -1 if version higher than other
             0 if version equal to other
             1 if version lower than other
        """
        VERSION_BIGGER = -1
        VERSION_LOWER  =  1
        VERSION_EQUAL  =  0

        if ignore_current_version_post:
            version = re.sub(_IGNORE_POST_VERSION_PATTERN, '',version, re.IGNORECASE)
            other = re.sub(_IGNORE_POST_VERSION_PATTERN, '',other, re.IGNORECASE)
        if other == version:
            return VERSION_EQUAL

        other_parsed = pkg_resources.parse_version(other)
        version_parsed = pkg_resources.parse_version(version)
        if other_parsed == version_parsed:
            return VERSION_EQUAL

        if other_parsed < version_parsed:
            return VERSION_BIGGER
        else:
            return VERSION_LOWER

except:
    def is_stable_version(version:str)->bool:
        match = re.match(_IS_STABLE_VESRION_PATTERN, version, re.IGNORECASE)
        return match is not None


    def compare_version(other:str, version:str, ignore_current_version_post:bool=False)->int:
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

        VERSION_BIGGER = -1
        VERSION_LOWER  =  1
        VERSION_EQUAL  =  0

        try:
            if ignore_current_version_post:
                version = re.sub(_IGNORE_POST_VERSION_PATTERN, '',version, re.IGNORECASE)
                other = re.sub(_IGNORE_POST_VERSION_PATTERN, '',other, re.IGNORECASE)

            if other != version:
                other_list, other_pre_post_list = _normalize_version(other)
                version_list, version_pre_post_list = _normalize_version(version)
                max_len = max(len(version_list), len(other_list))
                other_list = [*other_list, *([0] * max(0, max_len - len(other_list))), *other_pre_post_list]
                version_list = [*version_list, *([0] * max(0, max_len - len(version_list))), *version_pre_post_list]

                for idx in range(0, max_len):
                    v_element = int(version_list[idx])
                    o_element = int(other_list[idx])
                    if v_element != o_element:
                        if v_element > o_element:
                            return VERSION_BIGGER
                        else:
                            return VERSION_LOWER
                                  
        except:
            pass
        return VERSION_EQUAL


    def _normalize_version(v:str)->Tuple[list,list]:
        PRE_POST_STR_TO_LEVEL = {
            "dev": -40,
            "a": -30,
            "alpha": -30,
            "b": -20,
            "beta": -20,
            "rc": -10,

            "post": 10,
            "r": 10,
            "rev": 10
        }
        v = v.strip().lower()
        v = v[1:] if v.startswith("v") else v
        
        version_list = v.split(".")

        first = version_list[0].strip()
        pair = first.split("!", 1)
        if len(pair) == 1:
            version_list = [0, *version_list]
        else:
            version_list = [*pair, *version_list[1:]]
        last = version_list[-1].strip()

        pre_post_list = [0,0]
        if not is_int(last):
            version_list = version_list[:-1]
            if last != "":
                match = re.match(r'([0-9]*)[\-_]?([a-z]*)[\-_]?([0-9]*)', last, re.IGNORECASE)
                if match:
                    groups = match.groups()
                    pre_post_level = PRE_POST_STR_TO_LEVEL.get(groups[1].lower())
                    if pre_post_level is not None:
                        if groups[0] != "":
                            version_list.append(groups[0])
                        pre_post_version = int(groups[2]) if groups[2] != "" else 0
                        pre_post_list = [pre_post_level, pre_post_version]
                else:
                    pre_post_list = [last, last]

        return version_list, pre_post_list


    def is_int(str_val:str)->bool:
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


    def to_int(str_val:str)->int:
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


def pre_version_label(v:str)->str:
    PRE_STR_TO_LABEL = {
        "dev": "Development",
        "a": "Alpha",
        "alpha": "Alpha",
        "b": "Beta",
        "beta": "Beta",
        "rc": "Release Candidate",
    }
    version_list = v.split(".")
    last = version_list[-1].strip()
    if last != "":
        match = re.match(r'([0-9]*)[\-_]?([a-z]*)[\-_]?([0-9]*)', last, re.IGNORECASE)
        if match:
            groups = match.groups()
            return PRE_STR_TO_LABEL.get(groups[1])


def execute_version_command()->MarkdownString:
    """ execute the version command.
    command just return a string with the version that will be displayed to the user

    Returns
    -------
    str
        A string with the current version
    """

    return MarkdownString(f"{Constants.MAGIC_PACKAGE_NAME} version: {__version__}", title="version")


def get_pypi_latest_version(package_name:str)->Tuple[str,str]:
    """ Retreives latest package version string for PyPI.

    Parameters
    ----------
    package_name : str
        Name of package as define in PyPI.

    Returns
    -------
    str, str or None, None
        The latest version string and latest stable string of package in PyPI, or None, None if fails to retrieve

    Raises
    ------
    RequestsException
        If request to PyPI fails.
    """

    #
    # reterive package data
    #

    json_response = _retreive_package_from_pypi(package_name)
    all_releases = json_response.get("releases").keys()
    all_releases_desc_sorted = sorted(all_releases, key=cmp_to_key(lambda v,o: compare_version(v, o, False)), reverse=True)

    latest_stable_release = latest_release = json_response.get("info").get("version") if json_response and json_response.get("info") else None

    if len(all_releases_desc_sorted) > 0:
        latest_release = all_releases_desc_sorted[0]
        latest_stable_release = _get_latest_stable_version(all_releases_desc_sorted) or latest_stable_release or latest_release

    return latest_release, latest_stable_release


def _get_latest_stable_version(all_releases_desc_sorted:Iterable)->str:
    for release in all_releases_desc_sorted:
        if is_stable_version(release):
            return release


def _retreive_package_from_pypi(package_name:str)->Dict[str,Any]:
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
    http_client = HttpClient()
    response = http_client.get(api_url)

    #
    # handle response
    #

    response.raise_for_status()
    json_response = response.json()
    return json_response


def validate_required_python_version_running(minimal_required_version:str)->None:
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
        min_py_version = 1000000 * int(parts[0]) + 1000 * (int(parts[1]) if len(parts) > 1 else 0) + (int(parts[2]) if len(parts) > 2 else 0)
        running_py_version = 1000000 * sys.version_info.major + 1000 * sys.version_info.minor + sys.version_info.micro
        if running_py_version < min_py_version:
            raise RuntimeError("")
    except:
        raise RuntimeError(f"Kqlmagic requires python >= {Constants.MINIMAL_PYTHON_VERSION_REQUIRED}, you use python {sys.version}")
