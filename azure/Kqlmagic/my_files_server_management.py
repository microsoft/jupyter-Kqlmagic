# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------


import sys
import os
import subprocess as sub


import requests


from .constants import Constants
from .display import Display


DEFAULT_PROTOCOL = "http"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = "5000"


class FilesServerManagement(object):

    def __init__(self, server_py_code, server_url, base_folder, folders, options):
        protocol, host, port = self.pasre_server_url(server_url)
        self._server_py_code = server_py_code
        self._protocol = protocol or DEFAULT_PROTOCOL
        self._host = host or DEFAULT_HOST
        self._port = port or self.get_notused_port(host=self._host) or DEFAULT_PORT
        self._base_folder = base_folder
        self._folders = folders # comma separated folders
        self._server_url = f"{self._protocol}://{self._host}:{self._port}"
        self._is_registered = False
        self._is_started = False


    def get_notused_port(self, host=""):
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((host,0))
            s.listen(1)
            port = s.getsockname()[1]
            s.close()
            return port
        except:
            pass

    def pasre_server_url(self, url: str):
        protocol = host = port = None
        if url is not None:
            url = url.lower()
            parts = url.split("://", 1)
            if len(parts) == 2:
                protocol = parts[0]
                url = parts[1]
            parts = url.split(":", 1)
            if len(parts) == 2:
                host = parts[0] if len(parts[0]) > 0 else None
                port = parts[1] if len(parts[1]) > 0 else None
            else:
                try:
                    port = int(parts[0])
                except:
                    host = parts[0]
        return protocol, host, port

    @property
    def files_url(self) -> str:
        return f"{self._server_url}/files"


    @property
    def folders_url(self) -> str:
        return f"{self._server_url}/folders"

    @property
    def server_url(self) -> str:
        return self._server_url


    def startServer(self):
        if not self._is_started or not self.pingServer():
            python_exe = sys.executable or 'python'
            os.environ['FLASK_ENV'] = "development"
            window_visibility = os.getenv(f'{Constants.MAGIC_CLASS_NAME_UPPER}_FILES_SERVER_WINDOW_VISIBILITY')
            show_window = window_visibility is not None and window_visibility.lower() == 'show'
            if not show_window:
                # must use double quotes for base folder and folders, to allow spaces (note single quoted does not work)
                command = f'{python_exe} {self._server_py_code} -protocol={self._protocol} -host={self._host} -port={self._port} -base_folder="{self._base_folder}" -folders="{self._folders}" -parent_id="{os.getpid()}" -clean="folders"'
                sub.Popen(command, shell=True)
            else:
                # must use double quotes for base folder and folders, to allow spaces (note single quoted does not work)
                command = f'start /min /wait {python_exe} {self._server_py_code} -protocol={self._protocol} -host={self._host} -port={self._port} -base_folder="{self._base_folder}" -folders="{self._folders}" -parent_id="{os.getpid()}" -clean="folders"'
                sub.Popen(command, shell=True)

            self._is_started = True

        if not self._is_registered:
            Display._register_to_ipython_atexit(FilesServerManagement._abortServer, self._server_url)
            self._is_registered = True


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
