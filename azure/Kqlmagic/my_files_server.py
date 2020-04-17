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

        count = 1

        while True:
            try:
                if count == 0:
                    count = 60
                    print(f">>> ParentUnixMonitor for ppid {os.getppid()}, parent is running.")
                count -= 1

                time.sleep(1.0)

                if os.getppid() == 1:
                    print(f">>> Parent appears to have exited, shutting down.")
                    os_exit(0)
                    break

            except OSError as e:
                if e.errno != EINTR:
                    error_message = f"ParentUnixMonitor failed. error: {e}"
                    print(f">>> {error_message}, continue monitoring.")
                    # raise

            except Exception as ex:
                error_message = f"ParentUnixMonitor failed. error: {ex}"
                print(f">>> {error_message}, continue monitoring.")



class ParentWindowsMonitor(Thread):
    """ A  Windows-specific daemon thread that terminates the program immediately
    when the parent process no longer exists.
    """


    def __init__(self, parent_id=None):
        super(ParentWindowsMonitor, self).__init__()

        self.daemon = True
        self.parent_id = parent_id
        self.parent_create_time = None


    def run(self):
        """ Run the monitor loop. This method never returns.
        """
        import psutil
        from errno import EINTR

        count = 0
        # Listen forever.
        while True:
            try:

                if count == 0:
                    count = 60
                    print(f">>> ParentWindowsMonitor for ppid {self.parent_id}, parent is running.")
                count -= 1

                time.sleep(1.0)

                parent_proc_not_found = True
                for proc in psutil.process_iter():
                    if proc.pid == self.parent_id:
                        proc_create_time = proc.create_time()
                        self.parent_create_time = self.parent_create_time or proc_create_time
                        # check proc.create_time to make sure the parent_id is not resued
                        if self.parent_create_time == proc_create_time:
                            parent_proc_not_found = False
                            break

                if parent_proc_not_found:
                    print(">>> Parent appears to have exited, shutting down.")
                    os_exit(0)
                    break
                    
            except OSError as e:
                if e.errno != EINTR:
                    error_message = f"ParentWindowsMonitor failed. error: {e}"
                    print(f">>> {error_message}, continue monitoring.")
                    # raise

            except Exception as ex:
                error_message = f"ParentWindowsMonitor failed. error: {ex}"
                print(f">>> {error_message}, continue monitoring.")



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
    try:
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
        response.headers["Expires"] = 0
        response.headers["Pragma"] = "no-cache"

    except Exception as ex:
        error_message = f"after_request_func failed. error: {ex}"
        print(f">>> {error_message}.")
        pass

    return response


@app.route('/files/<foldername>/<kernel_id>/<filename>')
def files(foldername, kernel_id, filename):
    """return content of filename as the response body."""
    try:
        _kernel_id = request.args.get("kernelid")

        if len(folderlist) == 0 or f"{foldername}/{kernel_id}" in folderlist:
            err_resp = check_path(foldername, kernel_id, filename)
            if err_resp is not None:
                return err_resp
            file_path = f"{base_folder}/{foldername}/{kernel_id}/{filename}"

            return send_file(file_path)

        else:
            error_message = f"folder {foldername} not in {folderlist}"
            print(f">>> {error_message}.")
            return make_response(f"{error_message}, internal error", 404)

    except Exception as ex:
        error_message = f"/files/<foldername>/<kernel_id>/<filename> failed, error: {ex}"
        print(f">>> {error_message}.")
        return make_response(f"{error_message}, internal error", 500)


@app.route('/folders/<foldername>')
def folders(foldername):
    """return content of filename as the response body."""
    try:
        _kernel_id = request.args.get("kernelid")
        _filename = request.args.get("filename")

        if len(folderlist) == 0 or f"{foldername}/{_kernel_id}" in folderlist:
            err_resp = check_path(foldername, _kernel_id, _filename)
            if err_resp is not None:
                return err_resp
            file_path = f"{base_folder}/{foldername}/{_kernel_id}/{_filename}"
            return send_file(file_path)

        else:
            error_message = f"folder {foldername} not in {folderlist}"
            print(f">>> {error_message}.")
            return make_response(f"{error_message}, internal error", 404)

    except Exception as ex:
        error_message = f"/folders/<foldername> failed. error: {ex}."
        print(f">>> {error_message}.")
        return make_response(f"{error_message}, internal error", 500)
    


@app.route('/ping')
def ping():
    """print 'pong' as the response body."""
    try:
        _kernel_id = request.args.get("kernelId")

        return f'pong kernelid: {_kernel_id}'

    except Exception as ex:
        error_message = f"/ping failed. error: {ex}"
        print(f">>> {error_message}.")
        return make_response(f"{error_message}, internal error", 500)
    

@app.route('/webbrowser')
def webbrowser():
    """open web browser."""
    try:
        _encoded_url = request.args.get("url")
        _kernel_id = request.args.get("kernelId")

        import urllib.parse
        import webbrowser

        url = urllib.parse.unquote(_encoded_url)
        webbrowser.open(url, new=1, autoraise=True)

    except Exception as ex:
        error_message = f"/ping failed. error: {ex}"
        print(f">>> {error_message}.")
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
    try:
        _kernel_id = request.args.get("kernelId")

        os_exit(0)

    except Exception as ex:
        error_message = f"/abort failed. error: {ex}"
        print(f">>> {error_message}.")
        raise ex

    return ''


def check_path(foldername, kernel_id, filename):
    err_resp = None
    try:
        error_message = None
        if not os.path.exists(f"{base_folder}"):
            error_message = f"Base folder '{base_folder}' not found"

        elif not os.path.exists(f"{base_folder}/{foldername}"):
            error_message = f"Folder {base_folder}/{foldername} not found"

        elif not os.path.exists(f"{base_folder}/{foldername}/{kernel_id}"):
            error_message = f"Folder {base_folder}/{foldername}/{kernel_id}  not found"

        elif not os.path.exists(f"{base_folder}/{foldername}/{kernel_id}/{filename}"):
            error_message = f"File {base_folder}/{foldername}/{kernel_id}/{filename} not found"

        if error_message is not None:
            err_resp = make_response(error_message, 404)
            print(f">>> check_path failed, {error_message}. code: 404.")

    except Exception as ex:
        error_message = f"File {base_folder}/{foldername}/{kernel_id}/{filename}, error: {ex}"
        print(f">>>check_path failed, {error_message}, code: 500")
        err_resp = make_response(f"{error_message}, internal error", 500)

    return err_resp


def init_parent_monitor(parent_id):
    if parent_id is not None:
        try:
            parent_id = int(parent_id) 

            if sys.platform == 'win32':
                return ParentWindowsMonitor(parent_id)

            elif parent_id != 1:
                # PID 1 (init) is special and will never go away,
                # only be reassigned.
                # Parent polling doesn't work if ppid == 1 to start with.
                return ParentUnixMonitor()
        
        except Exception as ex:
            error_message = f"init_parent_monitor got an error: {ex}, parent monitor disabled"
            print(f">>> {error_message}.")


def os_exit(code):
    global os_exit_started
    if os_exit_started is None:
        os_exit_started = True
        try:
            print(f'>>> OS_EXIT code: {code}!!!')
            to_clean = params.get("clean", None)
            if to_clean is not None and len(folderlist) > 0:
                for folder in folderlist:
                    try:
                        folder_path = f"{base_folder}/{folder}"
                        print(f'>>> start to clean {folder_path}')
                        for filename in os.listdir(folder_path):
                            try:
                                file_path = f"{base_folder}/{folder}/{filename}"
                                os.unlink(file_path)
                            except:
                                print(f'>>> Failed to clean {file_path}, reason: {e}')
                        os.rmdir(folder_path)
                        print(f'>>> done to clean {folder_path}')
                    except Exception as e:
                        print(f'>>> Failed to clean {folder_path}, reason: {e}')
        except:
            print('>>> Failed to delete to clean')
        print(f'>>> done all clean')
        # time.sleep(60.0)
        os._exit(code)


if __name__ == "__main__":
    try:
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
                print(f">>> start parent_monitor for parent id: {parent_id}")
                parent_monitor.start()
            print(f" * parent id: {parent_id}")
            print(f" * Base folder: {base_folder}")
            print(f" * Folder list: {folderlist}")
            print(f" * ")
            app.run(host=host, port=port, debug=False, use_reloader=False, use_debugger=False)

        else:
            error_message = f"__name__ failed. base_folder value is None"
            print(f">>> {error_message}.")
            time.sleep(5 * 60 * 1.0)
            os._exit(1)

    except Exception as ex:
        error_message = f"__name__ failed. error: {ex}"
        print(f">>> {error_message}.")
        time.sleep(5 * 60 * 1.0)
        os._exit(1)
