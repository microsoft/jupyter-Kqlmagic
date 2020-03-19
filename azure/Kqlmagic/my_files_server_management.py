# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------


import sys
import os
import subprocess as sub


import requests


from .display import Display


class FilesServerManagement(object):


    def __init__(self, server_py_code, protocol, host, port, base_folder, folders, **kwargs):
        self._server_py_code = server_py_code
        self._protocol = protocol
        self._host = host
        self._port = port
        self._base_folder = base_folder
        self._folders = folders 
        self._server_url = f"{self._protocol}://{self._host}:{self._port}"


    @property
    def files_url(self) -> str:
        return f"{self._server_url}/files"


    @property
    def folders_url(self) -> str:
        return f"{self._server_url}/folders"


    def startServer(self):
        if not self.pingServer():
            command = f"start /min /wait  python {self._server_py_code} -protocol {self._protocol} -host {self._host} -port {self._port} -base_folder {self._base_folder} -folders {self._folders}"
            sub.Popen(command, shell=True)

        Display._register_to_ipython_atexit(FilesServerManagement._abortServer, self._server_url)


    def pingServer(self):
        try:
            ping_url = f"{self._server_url}/ping"
            requests.get(ping_url)
            return True
        except:
            return False


    def abortServer(self):
        FilesServerManagement._abortServer(self._server_url)


    @staticmethod
    def _abortServer(server_url):
        try:
            abort_url = f"{server_url}/abort"
            requests.get(abort_url)
        except:
            None
