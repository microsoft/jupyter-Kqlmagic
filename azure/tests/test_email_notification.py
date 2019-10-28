#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

import pytest
from azure.Kqlmagic.constants import Constants
from azure.Kqlmagic.kql_magic import Kqlmagic as Magic
from textwrap import dedent
import os
import re
from azure.Kqlmagic.constants import Constants 
from azure.Kqlmagic.email_notification import EmailNotification

import uuid

ip = get_ipython() # pylint:disable=undefined-variable


def test_validate_email_params_no_params():
    try:
        EmailNotification(params={})
    except ValueError:
        assert True
        return
    assert False

def test_validate_email_params_bad_params():
    params = {
        "smtpport": "123345", 
        "smtpendpoint": '123345', 
        "sendfrom": '123345', 
        "sendto": '123345', 
        "sendfrompassword": '123345', 
        "context": '123345'  
    }

    try:
        EmailNotification(**params)
    except ValueError:
        assert True
        return
    assert False


def test_validate_email_params_good_params():
    params = {
        "smtpport": "123345", 
        "smtpendpoint": '123345', 
        "sendfrom": 'abc@abc.abc', 
        "sendto": 'abc@abc.abc', 
        "sendfrompassword": '123345', 
        "context": '123345'  
    }
    try:
        EmailNotification(**params)
    except ValueError:
        assert False
    assert True
