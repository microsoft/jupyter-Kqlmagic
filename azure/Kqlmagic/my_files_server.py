# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import sys
import os
import platform
from datetime import datetime


from flask import Flask, send_file, request, make_response

import os
import platform
import time
import signal
from threading import Thread
from _thread import interrupt_main


class ParentUnixMonitor(Thread):
    """ A Unix-specific daemon thread that terminates the program immediately
    when the parent process no longer exists.
    """

    def __init__(self):
        super(ParentUnixMonitor, self).__init__()
        self.daemon = True

    def run(self):
        # We cannot use os.waitpid because it works only for child processes.
        from errno import EINTR
        while True:
            try:
                if os.getppid() == 1:
                    print("Parent appears to have exited, shutting down.")
                    os_exit(1)
                    break
                time.sleep(1.0)
            except OSError as e:
                if e.errno != EINTR:
                    raise



class ParentWindowsMonitor(Thread):
    """ A  Windows-specific daemon thread that terminates the program immediately
    when the parent process no longer exists.
    """


    def __init__(self, parent_id=None):

        assert(parent_id)
        super(ParentWindowsMonitor, self).__init__()

        self.daemon = True
        self.parent_id = parent_id
        self.parent_create_time = None

    def run(self):
        """ Run the monitor loop. This method never returns.
        """
        import psutil
        from errno import EINTR
        # Listen forever.
        while True:
            try:
                parent_exist = False
                for proc in psutil.process_iter():
                    if proc.pid == self.parent_id:
                        proc_create_time = proc.create_time()
                        self.parent_create_time = self.parent_create_time or proc_create_time
                        # check proc.create_time to make sure the parent_id is not resued
                        if self.parent_create_time == proc_create_time:
                            parent_exist = True
                            break
                if parent_exist:
                    time.sleep(1.0)
                else:
                    print("Parent appears to have exited, shutting down.")
                    os_exit(1)
                    break
                    
            except OSError as e:
                if e.errno != EINTR:
                    raise


DEFAULT_PORT = "5000"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PROTOCOL = "http"


parent_monitor = None
base_folder = None
os_exit_started = None
params = {}
folderlist = []

app = Flask("kqlmagic_temp_files_server")


@app.after_request
def after_request_func(response):
    """disable server and client cache"""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response


@app.route('/files/<foldername>/<kernelid>/<filename>')
def files(foldername, kernelid, filename):
    """return content of filename as the response body."""
    if len(folderlist) == 0 or f"{foldername}/{kernelid}" in folderlist:
        err_resp = check_path(foldername, kernelid, filename)
        if err_resp is not None:
            return err_resp
        file_path = f"{base_folder}/{foldername}/{kernelid}/{filename}"
        # print(f">>> files file_path: {file_path}")
        return send_file(file_path)


@app.route('/folders/<foldername>')
def folders(foldername):
    """return content of filename as the response body."""
    kernelid = request.args.get("kernelid")
    filename = request.args.get("filename")
    if len(folderlist) == 0 or f"{foldername}/{kernelid}" in folderlist:
        err_resp = check_path(foldername, kernelid, filename)
        if err_resp is not None:
            return err_resp
        file_path = f"{base_folder}/{foldername}/{kernelid}/{filename}"
        # print(f">>> folders file_path: {file_path}")
        return send_file(file_path)


@app.route('/ping')
def ping():
    """print 'pong' as the response body."""
    return 'pong'

@app.route('/webbrowser')
def webbrowser():
    """open web browser."""
    encoded_url = request.args.get('url')
    # print(f">>> url: {encoded_url}")
    try:
        import urllib.parse
        import webbrowser
        url = urllib.parse.unquote(encoded_url)
        webbrowser.open(url, new=1, autoraise=True)
    except:
        pass
    return """<!DOCTYPE html>
            <html><body>
            <script>
            window.close();
            //setTimeout(function(){ window.close(); }, 1*1000);
            </script>
            </body></html>"""


@app.route('/abort')
def abort():
    """aborts the process"""
    os_exit(1)
    return ''


def check_path(foldername, kernelid, filename):
    err_resp = None
    if not os.path.exists(f"{base_folder}"):
        err_resp = make_response(f"Base folder '{base_folder}' not found", 404)

    elif not os.path.exists(f"{base_folder}/{foldername}"):
        err_resp = make_response(f"Folder {base_folder}/{foldername} not found", 404)

    elif not os.path.exists(f"{base_folder}/{foldername}/{kernelid}"):
        err_resp = make_response(f"Folder {base_folder}/{foldername}/{kernelid}  not found", 404)

    elif not os.path.exists(f"{base_folder}/{foldername}/{kernelid}/{filename}"):
        err_resp = make_response(f"File {base_folder}/{foldername}/{kernelid}/{filename} not found", 404)
    return err_resp


def init_parent_monitor(parent_id):
    if parent_id is not None:
        parent_id = int(parent_id) 

        if sys.platform == 'win32':
            return ParentWindowsMonitor(parent_id)
        elif parent_id != 1:
            # PID 1 (init) is special and will never go away,
            # only be reassigned.
            # Parent polling doesn't work if ppid == 1 to start with.
            return ParentUnixMonitor()

def os_exit(code):
    global os_exit_started
    if os_exit_started is None:
        os_exit_started = True
        try:
            print(f'OS_EXIT code: {code}!!!')
            to_clean = params.get("clean", None)
            if to_clean is not None and len(folderlist) > 0:
                for folder in folderlist:
                    try:
                        folder_path = f"{base_folder}/{folder}"
                        print(f'start to clean {folder_path}')
                        for filename in os.listdir(folder_path):
                            try:
                                file_path = f"{base_folder}/{folder}/{filename}"
                                os.unlink(file_path)
                            except:
                                print(f'Failed to clean {file_path}, reason: {e}')
                        os.rmdir(folder_path)
                        print(f'done to clean {folder_path}')
                    except Exception as e:
                        print(f'Failed to clean {folder_path}, reason: {e}')
        except:
            print('Failed to delete to clean')
        print(f'done all clean')
        # time.sleep(60.0)
        os._exit(code)


if __name__ == "__main__":
    # print(f">>> argv: {sys.argv[1:]}")
    import time
    key = None
    for arg in sys.argv[1:]:
        kv = arg.split("=")
        if arg.startswith('-') and len(kv) == 2:
            key = kv[0][1:]
            value = kv[1]
            params[key] = value
            key= None

        elif key is not None:
            params[key] = arg
            key = None

        elif arg.startswith('-'):
            key = arg[1:]

    # print(f">>> params: {params}")
    base_folder = params.get("base_folder")       
    if base_folder is not None:
        base_folder = base_folder[:-1] if base_folder.endswith('/') else base_folder
        folders = params.get("folders", [])
        folderlist = [f[:-1] if f.endswith('/') else f for f in folders.split(",")]
        port = params.get("port", DEFAULT_PORT)
        host = params.get("host", DEFAULT_HOST)
        parent_id = params.get("parent_id", None) 
        parent_monitor = init_parent_monitor(parent_id)
        if parent_monitor is not None:
            parent_monitor.start()
        print(f" * Base folder: {base_folder}")
        print(f" * Folder list: {folderlist}")
        print(f" * ")
        app.run(host=host, port=port, debug=False, use_reloader=False, use_debugger=False)
