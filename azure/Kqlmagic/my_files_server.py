# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import sys
import os
from datetime import datetime


from flask import Flask, send_file, request, make_response


DEFAULT_PORT = "5000"
DEFAULT_HOST = "127.0.0.1"


base_folder = None
folderlist = []

app = Flask("kqlmagic_temp_files_server")


@app.after_request
def after_request_func(response):
    """disable server and client cache"""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response


@app.route('/files/<foldername>/<filename>')
def files(foldername, filename):
    """return content of filename as the response body."""
    if len(folderlist) == 0 or foldername in folderlist:
        err_resp = check_path(foldername, filename)
        if err_resp is not None:
            return err_resp
        file_path = f"{base_folder}/{foldername}/{filename}"
        # print(f">>> files file_path: {file_path}")
        return send_file(file_path)


@app.route('/folders/<foldername>')
def folders(foldername):
    """return content of filename as the response body."""
    if len(folderlist) == 0 or foldername in folderlist:
        filename = request.args.get("filename")
        err_resp = check_path(foldername, filename)
        if err_resp is not None:
            return err_resp
        file_path = f"{base_folder}/{foldername}/{filename}"
        # print(f">>> folders file_path: {file_path}")
        return send_file(file_path)


@app.route('/ping')
def ping():
    """print 'pong' as the response body."""
    return 'pong'


@app.route('/abort')
def abort():
    """aborts the process"""
    os.abort()
    return ''


def check_path(foldername, filename):
    err_resp = None
    if not os.path.exists(f"{base_folder}"):
        err_resp = make_response("Base folder not found", 404)
    elif not os.path.exists(f"{base_folder}/{foldername}"):
        err_resp = make_response("Folder not found", 404)
    elif not os.path.exists(f"{base_folder}/{foldername}/{filename}"):
        err_resp = make_response("File not found", 404)
    return err_resp


if __name__ == "__main__":
    params = {}
    key = None
    for arg in sys.argv[1:]:
        if key is not None:
            params[key] = arg
            key = None
        elif arg.startswith('-'):
            key = arg[1:]

    base_folder = params.get('base_folder')       
    if base_folder is not None:
        base_folder = base_folder[:-1] if base_folder.endswith('/') else base_folder
        folders = params.get('folders', [])
        folderList = [f[:-1] if f.endswith('/') else f for f in folders.split(",")]
        port = params.get('port', DEFAULT_PORT)
        host = params.get('host', DEFAULT_HOST)
        app.run(host=host, port=port)
