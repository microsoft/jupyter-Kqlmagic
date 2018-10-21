#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

""" Constants file. """
class Constants(object):
    MAGIC_SOURCE_REPOSITORY_NAME = 'https://github.com/Microsoft/jupyter-Kqlmagic'
    MAGIC_PIP_REFERENCE_NAME = 'Kqlmagic' # 'git+git://github.com/Microsoft/jupyter-Kqlmagic.git'
    MAGIC_PACKAGE_NAME = 'Kqlmagic'
    MAGIC_CLASS_NAME = 'Kqlmagic'
    MAGIC_NAME = 'kql'
    MAGIC_ALIASES = []
    LOGGER_NAME = "{0}-py".format(MAGIC_CLASS_NAME)

class ConnStrKeys(object):
    TENANT = "tenant"
    USERNAME = "username"
    PASSWORD = "password"
    CLIENTID = "clientid"
    CLIENTSECRET = "clientsecret"
    CERTIFICATE = "certificate"
    CERTIFICATE_PEM_FILE = "certificate_pem_file"
    CERTIFICATE_THUMBPRINT = "certificate_thumbprint"
    APPKEY = "appkey"
    CODE = "code"

    CLUSTER = "cluster"
    DATABASE = "database"
    WORKSPACE = "workspace"
    APPID = "appid"

    ALIAS = "alias"
