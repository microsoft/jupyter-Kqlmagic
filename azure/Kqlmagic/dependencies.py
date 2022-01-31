# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Any, Dict, List, Type
import warnings
import re
import platform


from ._debug_utils import debug_print
from ._require import EXTRAS_REQUIRE, INSTALL_REQUIRES, list_union, strip_package_name
from .constants import Constants
from .log import logger, isNullLogger
from .my_utils import get_env_var_list, get_env_var_bool


saved_formatwarning = warnings.formatwarning



class Dependencies(object):

    IGNORE_WARNINGS = "ignore"
    SHOW_WARNINGS = "default"
    RAISE_WARNINGS = "error"

    PREFIX_TAG = "###"
    MANDATORY_TAG = "#M#"  # Kqlmagic cannot work without it
    BASE_TAG = "#B#"       # Kqlmagic base feature, however they may be disabled
    OPTIONAL_TAG = "#O#"   # Kqlmagic optional feature
    EXTRA_TAG = "#E#"
    DISABLED_TAG = "#D#"

    MANDATORY_MODULES_REGEX = r'^' + MANDATORY_TAG
    BASE_MODULES_REGEX = r'^' + BASE_TAG
    OPTIONAL_MODULES_REGEX = r'^' + OPTIONAL_TAG
    EXTRA_MODULES_REGEX = r'^' + EXTRA_TAG
    DISABLED_MODULES_REGEX = r'^' + DISABLED_TAG
    ALL_MODULES_REGEX = r'^(' + f'{MANDATORY_TAG}|{BASE_TAG}|{OPTIONAL_TAG}|{EXTRA_TAG}|{DISABLED_TAG}' + r')'

    MISSING_ENV_TAG = "#V#"
    MISSING_ENV_REGEX = r'^' + MISSING_ENV_TAG

    BAD_ENV_TAG = "#A#"
    BAD_ENV_REGEX = r'^' + BAD_ENV_TAG


    VERSION_IN_MODULE=True

    # platforms: 'Linux' | 'Windows' | 'Darwin' | 'Java' | ''
    platform_dependencies = {
        "Windows": (
            ('msal_extensions.windows', 'msal_extensions', OPTIONAL_TAG, "won't be able to authenticate using msal Single-Sign-On authentication modes", 'msal_extensions'),
            # ('winwin', 'winwin', OPTIONAL_TAG, "will be dsiabled", VERSION_IN_MODULE),
        ),
        "Linux": (
            ('msal_extensions.libsecret', 'msal_extensions', OPTIONAL_TAG, "won't be able to authenticate using msal Single-Sign-On authentication modes", 'msal_extensions'),
            # ('winwin', 'winwin', OPTIONAL_TAG, "will be dsiabled", VERSION_IN_MODULE),
        ),
        "Darwin": (
            ('msal_extensions.osx', 'msal_extensions', OPTIONAL_TAG, "won't be able to authenticate using msal Single-Sign-On authentication modes", 'msal_extensions'),
            # ('winwin', 'winwin', OPTIONAL_TAG, "will be dsiabled", VERSION_IN_MODULE),
        )
    }

    dependencies: list = [
        ('dateutil.parser', 'python-dateutil', MANDATORY_TAG, "won't be able handle datetime properly", 'dateutil'),
        ('traitlets', 'traitlets', MANDATORY_TAG, "won't be able to use execute Kqlmagic", VERSION_IN_MODULE),

        ('prettytable', 'prettytable', OPTIONAL_TAG, "won't be able to display tables", VERSION_IN_MODULE),
        # ('tabulate', 'tabulate', OPTIONAL_TAG, "won't be able to display tables", VERSION_IN_MODULE),
        ('requests', 'requests', OPTIONAL_TAG, "will use urllib, that might have ssl and or proxies restrictions", VERSION_IN_MODULE),
        ('msal', 'msal', OPTIONAL_TAG, "won't be able to authenticate using msal authentication modes, and Kqlmagic sso will be disabled", VERSION_IN_MODULE),
        ('azure.identity', 'azure-identity', OPTIONAL_TAG, "Some authentication options won't be available", VERSION_IN_MODULE),
        ('pandas', 'pandas', OPTIONAL_TAG, "won't be able to use dataframes", VERSION_IN_MODULE),
        ('IPython', 'ipython', OPTIONAL_TAG, "won't be to execute as an jupyter magic", VERSION_IN_MODULE),
        ('ipykernel', 'ipykernel', OPTIONAL_TAG, "won't be to execute as an jupyter magic on some jupyter variants", VERSION_IN_MODULE),
        ('pygments', 'pygments', OPTIONAL_TAG, "json objects won't be decorated with colors", VERSION_IN_MODULE),
        ('pygments.lexers.data', 'pygments', OPTIONAL_TAG, "json objects won't be decorated with colors", 'pygments'),
        ('pygments.formatters.terminal', 'pygments', OPTIONAL_TAG, "json objects won't be decorated with colors", 'pygments'),
        ('pyperclip', 'pyperclip', OPTIONAL_TAG, "copy/paste feature will be disabled in device code authentication", VERSION_IN_MODULE),
        ('azure.common.credentials', 'azure-common', OPTIONAL_TAG, "-try_azcli_login, -try_azcli_login_subscription and -try_azcli_login_by_profile authentication options will be dsiabled", 'azure.common'),
        ('msrestazure.azure_active_directory', 'msrestazure', OPTIONAL_TAG, "-try_msi authentication options will be dsiabled", 'msrestazure'),
        ('psutil', 'psutil', OPTIONAL_TAG, "some jupyter variants may not be detected correctly", VERSION_IN_MODULE),

        ('matplotlib.pyplot', 'matplotlib', DISABLED_TAG, 'plotting with matplotlib will be dsiabled', 'matplotlib'),

        ('matplotlib.cm', 'matplotlib', OPTIONAL_TAG, "matplotlib color maps (palettes) won't be available in plots", 'matplotlib'),
        ('matplotlib.colors', 'matplotlib', OPTIONAL_TAG, "matplotlib color maps (palettes) won't be available in plots", 'matplotlib'),
        ('pkg_resources', 'setuptools', OPTIONAL_TAG, 'may not detect new version exist in PyPI', 'setuptools'),
        ('plotly', 'plotly', OPTIONAL_TAG, "won't display charts with plotly", VERSION_IN_MODULE),
        ('plotly.graph_objs', 'plotly', OPTIONAL_TAG, "won't display charts with plotly", 'plotly'),
        ('flask', 'flask', OPTIONAL_TAG, "popups and some authentication modes won't work on some local jupyter variants", VERSION_IN_MODULE),
        ('isodate', 'isodate', OPTIONAL_TAG, "Azure Monitor AI/LA timespan specified with python timedate.timedelta object will be disabled", VERSION_IN_MODULE),
        ('markdown', 'markdown', OPTIONAL_TAG, "in text jupyter implementations, some help information might not be nicely displayed", VERSION_IN_MODULE),
        ('bs4', 'beautifulsoup4', OPTIONAL_TAG, "in text jupyter implementations, some help information might not be nicely displayed", VERSION_IN_MODULE),
        ('lxml', 'lxml', OPTIONAL_TAG, "in text jupyter implementations, some help information won't be nicely displayed", VERSION_IN_MODULE),
        ('ipywidgets', 'ipywidgets', OPTIONAL_TAG, "widget features will be disabled", VERSION_IN_MODULE),
        ('cryptography.fernet', 'cryptography', OPTIONAL_TAG, "Single Sign On feature will be disabled", 'cryptography'),
        ('cryptography.hazmat.backends', 'cryptography', OPTIONAL_TAG, "Single Sign On feature will be disabled", 'cryptography'),
        ('cryptography.hazmat.primitives', 'cryptography', OPTIONAL_TAG, "Single Sign On feature will be disabled", 'cryptography'),
        ('cryptography.hazmat.primitives.kdf.pbkdf2', 'cryptography', OPTIONAL_TAG, "Single Sign On feature will be disabled", 'cryptography'),
        ('password_strength', 'password_strength', OPTIONAL_TAG, "Single Sign On feature will be disabled", VERSION_IN_MODULE),
    ]

    dependencies_dict = {entry[0]:entry for entry in dependencies}


    installed_modules:Dict[str,Any] = {}
    installed_versions:Dict[str,str] = {}

    extras_names:List[str] = []
    extras_require_packages:List[str] = []
    extras_require_package_names:List[str] = []

    install_requires_packages:List[str] = []
    install_requires_package_names:List[str] = []

    install_package_names:List[str] = []

    is_only_installed_packages:bool = False
    debug_disabled_packages:List[str] = []


    def __init__(self)->None:
        self.extend_dependencies()
        self.set_installed_modules_and_versions()
        # Dependencies.installed_modules["pandas"] = False
        # Dependencies.installed_modules["plotly"] = False


    @classmethod
    def extend_dependencies(cls)->None:
        key = platform.system()
        cls.dependencies.extend(cls.platform_dependencies.get(key, ()))


    @classmethod
    def set_installed_modules_and_versions(cls)->None:
        cls.debug_disabled_packages = [name.strip() for name in (get_env_var_list(f"{Constants.MAGIC_CLASS_NAME_UPPER}_DEBUG_DISABLE_PACKAGES") or [])]
        cls.is_only_installed_packages = get_env_var_bool(f"{Constants.MAGIC_CLASS_NAME_UPPER}_ONLY_INSTALLED_PACKAGES", False)

        cls.extras_names = ([name.strip() for name in (get_env_var_list(f"{Constants.MAGIC_CLASS_NAME_UPPER}_EXTRAS_REQUIRE") or [])] or
                            [name.strip() for name in (get_env_var_list(f"{Constants.MAGIC_CLASS_NAME_UPPER}_EXTRAS_REQUIRES") or [])])

        if cls.extras_names:
            cls.extras_require_packages = list_union(*[EXTRAS_REQUIRE.get(name, []) for name in cls.extras_names])
        else:
            cls.extras_require_packages = EXTRAS_REQUIRE.get('default', [])
        cls.extras_require_package_names = list(map(strip_package_name, cls.extras_require_packages))

        cls.install_requires_packages = INSTALL_REQUIRES
        cls.install_requires_package_names = list(map(strip_package_name, cls.install_requires_packages))

        cls.install_package_names = list_union(cls.install_requires_package_names, cls.extras_require_package_names)

        for item in cls.dependencies:
            module_name = item[0]
            package_name = item[1]
            version_location = item[4]
            cls.get_module(module_name, package_name=package_name, version_location=version_location, dont_throw=True)
        logger().debug(f"installed versions: {cls.installed_versions}")


    @classmethod
    def is_installed(cls, module_name:str)->bool:
        return cls.get_module(module_name, dont_throw=True) is not None


    @classmethod
    def installed_packages(cls)->Dict[str,str]:
        return {package: cls.installed_versions.get(package) for package in cls.installed_versions}



    @classmethod
    def get_module(cls, module_name:str, package_name:str=None, version_location:str=None, dont_throw:bool=False, message:str=None):
        try:
            module = cls.installed_modules.get(module_name)
            if module:
                return module

            elif module is False:
                raise Exception("not_installed")

            if cls.debug_disabled_packages and (package_name.replace("_", "-") in cls.debug_disabled_packages or package_name.replace("-", "_") in cls.debug_disabled_packages):
                raise Exception("debug_disabled_package")
                
            if cls.is_only_installed_packages and package_name.replace("_", "-") not in  cls.install_package_names and package_name.replace("-", "_") not in  cls.install_package_names:
                debug_print(f">>> package {package_name} disabled by due to  only install packages set to True")
                raise Exception(f"package {package_name} disabled by due to  only install packages set to True")

            import importlib
            module = importlib.import_module(module_name)
            cls.installed_modules[module_name] = module
            if package_name is not None and cls.installed_versions.get(package_name) is None:
                version = None
                if version_location is not None:
                    try:
                        if version_location == cls.VERSION_IN_MODULE:
                            version_module = module
                        else:
                            # version_location should refer to a module 
                            version_module = cls.installed_modules.get(version_location)
                            if version_module is None:
                                version_module = importlib.import_module(version_location)
                                cls.installed_modules[version_location] = version_module
                        version = version_module.__version__
                    except:
                        try:
                            import pkg_resources  # part of setuptools
                            version = pkg_resources.require(package_name)[0].version
                        except:
                            pass
                cls.installed_versions[package_name] = version or "?.?.?"
            return module
        except Exception as error:
            cls.installed_modules[module_name] = False
            if f"{error}" == "debug_disabled_package":
                debug_disabled_package_message = f"package '{package_name}' was disabled, because specified in enironment variable {Constants.MAGIC_CLASS_NAME_UPPER}__DEBUG_DISABLE_PACKAGES"
                print(f"!!! Note: {debug_disabled_package_message} !!!")
                message = f", {message}" if message else ""
                message = f"{debug_disabled_package_message}{message}"
            if dont_throw:
                return None
            item = cls.dependencies_dict.get(module_name)
            message = f", {message}" if message else ""
            import_error_message = cls._import_error_message(*item) if item else f"due to '{module_name}' module/package not installed"
            import_error_message = f"{import_error_message}{message}"
            raise NotImplementedError(import_error_message)


    @classmethod
    def warn_missing_dependencies(cls, options:Dict[str,Any]=None)->None:
        options = options or {}
        if not isNullLogger:
            for item in cls.dependencies:
                module_name = item[0]
                if cls.is_installed(module_name):
                    package_name = item[1]
                    tail = f"installed version '{cls.installed_versions.get(package_name)}'"
                else:
                    tail = "NOT installed"
                logger().debug(f"dependency -- '{item}' -- {tail}")

        warnings.filterwarnings(cls.RAISE_WARNINGS, message=cls.MANDATORY_MODULES_REGEX, category=ImportWarning)

        warnings.filterwarnings(cls.SHOW_WARNINGS, message=cls.BASE_MODULES_REGEX, category=ImportWarning)

        warnings.filterwarnings(cls.SHOW_WARNINGS, message=cls.OPTIONAL_MODULES_REGEX, category=ImportWarning)

        warnings.filterwarnings(cls.IGNORE_WARNINGS, message=cls.EXTRA_MODULES_REGEX, category=ImportWarning)

        warnings.filterwarnings(cls.IGNORE_WARNINGS, message=cls.DISABLED_MODULES_REGEX, category=ImportWarning)
        # warnings.simplefilter("always")
        warnings.formatwarning = cls._formatwarning

        if cls.extras_names:
            cls.warn_unknown_extras_require_name(cls.extras_names)

        for item in cls.dependencies:
            cls._import_warn(cls.install_package_names, *item)


    @classmethod
    def _import_error_message(cls, module_name:str, package:str, tag:str, message:str, version_location:str)->str:
        return f"failed to import '{module_name}' from '{package}', {message}"


    @classmethod
    def _import_warn(cls, install_package_names:List[str], module_name:str, package:str, tag:str, message:str, version_location:str)->None:
        if not cls.installed_modules.get(module_name) and package in install_package_names:
            import_error_message = cls._import_error_message(module_name, package, tag, message, version_location)
            warnings.warn_explicit(f"{tag}{import_error_message}", ImportWarning, '', 0)


    @classmethod
    def _formatwarning(cls, message:str, category:Type[Warning], filename:str, lineno:int, line:str=None)->str:
        global saved_formatwarning
        if category.__name__ == 'ImportWarning' and re.match(cls.ALL_MODULES_REGEX, f'{message}'):
            message = f'{message}'[3:]
            return f"{category.__name__}: {message}\n"
        elif category.__name__ == 'UserWarning' and re.match(cls.MISSING_ENV_REGEX, f'{message}'):
            message = f'{message}'[3:]
            # assumes env variable is passed as filename
            return f"MissingEnvironmentVariable: {filename} is not set. {message}\n"
        elif category.__name__ == 'UserWarning' and re.match(cls.BAD_ENV_REGEX, f'{message}'):
            message = f'{message}'[3:]
            # assumes env variable is passed as filename
            return f"BadEnvironmentVariable: {filename} value is invalid. {message}\n"
        else:
            return saved_formatwarning(message, category, filename, lineno, line=line)


    @classmethod
    def warn_missing_env_variables(cls, options:Dict[str,Any]=None)->None:
        options = options or {}
        if options.get("notebook_app") in ["azureml", "azuremljupyternotebook", "azuremljupyterlab"]:
            if options.get("notebook_service_address") is None:
                var_name = f"{Constants.MAGIC_CLASS_NAME_UPPER}_AZUREML_COMPUTE"
                message = f"{cls.MISSING_ENV_TAG}Popup windows might not work properly (copy/paste compute host value from the address bar of the notebook when activated in azureml Jupyter mode)"
                warnings.warn_explicit(f"{message}", UserWarning, var_name, 0)

    @classmethod
    def warn_unknown_extras_require_name(cls, extras_names:List[str])->None:
        unknown_names = [name for name in extras_names if name not in EXTRAS_REQUIRE]
        if unknown_names:
            var_name = f"{Constants.MAGIC_CLASS_NAME_UPPER}_EXTRAS_REQUIRE"
            message = f"{cls.BAD_ENV_TAG}unknown extras_requires {unknown_names}, wrong warning about missing modules might display"
            warnings.warn_explicit(f"{message}", UserWarning, var_name, 0)

