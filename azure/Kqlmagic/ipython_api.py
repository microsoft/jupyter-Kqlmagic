# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import sys
import time
from typing import Any, Callable, Iterable


from ._debug_utils import debug_print
try:
    try:
        from IPython import display as ipy_display
    except:
        import IPython.core.display as ipy_display
        
    display = ipy_display.display
    HTML = ipy_display.HTML
    Javascript = ipy_display.Javascript
    JSON = ipy_display.JSON
except:
    display = None
    HTML = None
    Javascript = None
    JSON = None


class IPythonAPI(object):
    """
    """

    kernel_id:str = None
    ip_initialized:bool = None
    ip = None
    ipywidgets_installed:bool = None


    @classmethod
    def is_ipywidgets_installed(cls)->bool:
        if cls.ipywidgets_installed is None:
            try:
                import ipywidgets
            except Exception:
                cls.ipywidgets_installed = False
            else:
                cls.ipywidgets_installed = True
        return cls.ipywidgets_installed


    @classmethod
    def get_notebook_kernel_id(cls)->str:
        if cls.kernel_id is None:
            try:
                import re
                try:
                    import ipykernel as kernel
                except:
                    from IPython.lib import kernel

                connection_file = kernel.get_connection_file()
                cls.kernel_id = re.search('kernel-(.*).json', connection_file).group(1)

            except:
                import uuid
                cls.kernel_id = f"{uuid.uuid4()}"

        return cls.kernel_id


    @classmethod
    def get_ipython_root_path(cls, **options)->str:
        temp_folder_location = options.get("temp_folder_location")
        root_path = None
        if temp_folder_location == "starting_dir":
            ip = cls._get_ipython()
            if ip is not None:
                root_path = ip.starting_dir
        if root_path is None:
            root_path = os.path.expanduser('~')
        return root_path


    @classmethod
    def _get_ipython(cls):

        if cls.ip is None and cls.ip_initialized is None:
            cls.ip_initialized = True
            if "IPython" in sys.modules:
                try:
                    from IPython import get_ipython
                except:
                    get_ipython = None
                    
                if get_ipython is not None:
                    try:
                        cls.ip = get_ipython()  # pylint: disable=undefined-variable 
                    except:
                        pass
        return cls.ip


    @classmethod
    def _get_ipython_help_links(cls, **options)->list:
        help_links = None
        ip = cls._get_ipython()
        if ip is not None and hasattr(ip, "kernel") and hasattr(ip.kernel, "_trait_values"):
            help_links = ip.kernel._trait_values.get("help_links")
        return help_links


    @classmethod
    def has_ipython_kernel(cls)->bool:
        has_kernel = False
        ip = cls._get_ipython()
        if ip is not None:
            has_kernel = hasattr(ip, "kernel")
        return has_kernel


    @classmethod
    def _get_ipython_db(cls, **options):
        db = None
        ip = cls._get_ipython()
        if ip is not None:
            db = ip.db
        return db


    @classmethod
    def try_kernel_execute(cls, javascript_statement:str, **options)->bool:
        if display is not None and Javascript is not None:
            display(Javascript(javascript_statement))
            return True
        else:
            return False


    @classmethod
    def try_kernel_reconnect(cls, **options)->bool:
        if options is None or options.get("notebook_app") not in ["jupyterlab", "visualstudiocode", "azuredatastudio", "azuredatastudiosaw", "nteract"]:
            result = cls.try_kernel_execute("""try {IPython.notebook.kernel.reconnect();} catch(err) {;}""")
            time.sleep(1)
            return result
        else:
            return False


    @classmethod
    def try_add_to_help_links(cls, text:str, url:str, reconnect:bool, **options)->bool:
        help_links = cls._get_ipython_help_links()
        if help_links is not None:
            found = False
            for link in help_links:
                # if found update url
                if link.get("text") == text:
                    if link.get("url") != url:
                        link["url"] = url
                    else:
                        reconnect = False
                    found = True
                    break
            if not found:
                help_links.append({"text": text, "url": url})
            if reconnect:
                return cls.try_kernel_reconnect(**options)
            else:
                return True
        else:
            return False


    @classmethod
    def try_add_to_ipython_tempfiles(cls, filename:str, **options)->bool:
        ip = cls._get_ipython()
        if ip is not None:
            ip.tempfiles.append(filename)
            return True
        else:
            return False


    @classmethod
    def try_add_to_ipython_tempdirs(cls, foldername:str, **options)->bool:
        ip = cls._get_ipython()
        if ip is not None:
            ip.tempdirs.append(foldername)
            return True
        else:
            return False


    @classmethod
    def try_register_to_ipython_atexit(cls, func, *args)->bool:
        ip = cls._get_ipython()
        if ip is not None:
            try:
                import atexit
                atexit.register(func, *args)  
            except:
                return False


    @classmethod
    def try_init_ipython_matplotlib_magic(cls, **options)->bool:
        matplotlib_magic_command = "inline" if options.get('notebook_app') != "ipython" else "qt"
        ip = cls._get_ipython()
        if ip is not None:
            try:
                ip.magic(f"matplotlib {matplotlib_magic_command}")
                return True
            except:
                return False


    @classmethod
    def get_shell(cls):
        return cls._get_ipython()


    @classmethod
    def run_cell_magic(cls, name:str, line:str, cell:str)->Any:
        ip = cls._get_ipython()
        if ip is not None:
            return ip.run_cell_magic(name, line, cell)
        else:
            raise Exception("not python kernel, can't execute cell magic")


    @classmethod
    def run_line_magic(cls, name:str, body:str)->Any:
        ip = cls._get_ipython()
        if ip is not None:
            return ip.run_line_magic(name, body)
        else:
            raise Exception("not python kernel, can't execute line magic")


    @classmethod
    def get_notebook_connection_info(cls):
        conn_info = None
        try:
            try:
                import ipykernel as kernel
            except:
                from IPython.lib import kernel
            conn_info = kernel.get_connection_info(unpack=False)

        except:
            pass

        return conn_info


    @classmethod
    def transform_cell(cls, cell:str)->str:
        tr_cell = cell
        try:
            ip = cls._get_ipython()
            if ip is not None:
                tr_cell = ip.input_transformer_manager.transform_cell(cell)
        except:
            pass
        return tr_cell


    @classmethod
    def is_in_input_transformers_cleanup(cls, transformer_func:Callable[[Iterable], Iterable])->bool:
        try:
            ip = cls._get_ipython()
            if ip is not None:
                return transformer_func in ip.input_transformers_cleanup
        except:
            pass
        return False


    @classmethod
    def try_add_input_transformers_cleanup(cls, transformer_func:Callable[[Iterable], Iterable])->bool:
        cls.try_remove_input_transformers_cleanup(transformer_func) # to make sure it within list only once, and add in the end of the list
        try:
            ip = cls._get_ipython()
            if ip is not None:
                ip.input_transformers_cleanup.append(transformer_func)
                return True
        except:
            pass
        return False


    @classmethod
    def try_remove_input_transformers_cleanup(cls, transformer_func:Callable[[Iterable], Iterable])->bool:
        try:
            ip = cls._get_ipython()
            if ip is not None:
                if cls.is_in_input_transformers_cleanup(transformer_func):
                    ip.input_transformers_cleanup.remove(transformer_func)
                return True
        except:
            pass
        return False

      
try:
    from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
    is_magics_class = True
except Exception:
    class Magics(object):
        def __init__(self, shell):
            pass
        
    def magics_class(_class):
        class _magics_class(object):
            def __init__(self, *args, **kwargs):
                self.oInstance = _class(*args,**kwargs)
            
            def __getattribute__(self,s):
                try:    
                    x = super(_magics_class,self).__getattribute__(s)
                except AttributeError:      
                    return self.oInstance.__getattribute__(s)
                else:
                    return x
        return _magics_class
    def cell_magic(_name):
        def _cell_magic(_func):
            return _func
        return _cell_magic
    def line_magic(_name):
        def _line_magic(_func):
            return _func
        return _line_magic
    def needs_local_scope(_func):
        return _func
    
    is_magics_class = False
