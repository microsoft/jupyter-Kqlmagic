# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

""" Constants file. """


class Constants(object):
    MAGIC_SOURCE_REPOSITORY_NAME = "https://github.com/Microsoft/jupyter-Kqlmagic"
    MAGIC_PIP_REFERENCE_NAME = "Kqlmagic"  # 'git+git://github.com/Microsoft/jupyter-Kqlmagic.git'
    MAGIC_PACKAGE_NAME = "Kqlmagic"
    
    # class must be uppercase, because %config can't find it if it is lowercase?
    MAGIC_CLASS_NAME = "Kqlmagic"
    MAGIC_NAME = "kql"
    MAGIC_ALIASES = []
    LOGGER_NAME = "{0}-py".format(MAGIC_CLASS_NAME)


class ConnStrKeys(object):
    # make sure all keys as lowercase, without spaces, underscores, and hyphen-minus
    # because they are ignored
    TENANT = "tenant"
    USERNAME = "username"
    PASSWORD = "password"
    CLIENTID = "clientid"
    CLIENTSECRET = "clientsecret"
    CERTIFICATE = "certificate"
    CERTIFICATE_PEM_FILE = "certificatepemfile"
    CERTIFICATE_THUMBPRINT = "certificatethumbprint"
    APPKEY = "appkey"
    CODE = "code"

    CLUSTER = "cluster"
    DATABASE = "database"
    WORKSPACE = "workspace"
    APPID = "appid"
    FOLDER = "folder"


    ALIAS = "alias"
