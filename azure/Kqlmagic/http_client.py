# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""Simple HTTP Client"""
import json
import gzip
from typing import Dict, Any, Union

from urllib.parse import urlencode

from .dependencies import Dependencies
from .constants import Constants
from .log import logger
from ._version import __version__


requests_module = Dependencies.get_module("requests", dont_throw=True)

class HTTPErrorResponse(object):

    global requests_module
    if requests_module is None:
        from urllib.error import HTTPError
        def __init__(self, http_error: HTTPError)-> None:
            self._http_error = http_error
            self._text = self._http_error.read() if hasattr(self._http_error, "read") and callable(self._http_error.read) else ""
            self._code = self._http_error.code if hasattr(self._http_error, "code") else 500
            self._headers = self._http_error.headers if hasattr(self._http_error, "headers") else {}


        def getcode(self)-> int:
            return self._code


        def info(self)-> dict:
            return _headers


        def read(self)-> bytes:
            return self._text


class Response(object):
    """This describes a minimal http response interface used by this package.
    :var int status_code:
        The status code of this http response.
        Our async code path would also accept an alias as "status".
    :var string text:
        The body of this http response.
        Our async code path would also accept an awaitable with the same name.
    """

    global requests_module
    if requests_module is None:
        from http.client import HTTPResponse

        def __init__(self, url: str, response: HTTPResponse)-> None:
            """
            :param response: The return value from a open call
                            on a urllib.build_opener()
            :type response:  urllib response object
            """
            self._url: str = url
            self._response = response
            self._status_code: int = None
            self._headers: dict = None
            self._content: bytes = None
            self._text: str = None


        @property
        def status_code(self)-> int:
            """status code"""
            self._status_code = self._status_code or self._response.getcode()
            return self._status_code


        @property
        def headers(self)-> dict:
            """response headers"""
            self._headers = self._headers or self._response.info()
            return self._headers


        @property
        def text(self)-> str:
            """response data string"""
            self._text = self._text or self._decode(self.content)
            return self._text


        @property
        def content(self)-> bytes:
            """response data content"""
            self._content = self._content or self._get_pagedata_bytes(self._response.read())
            return self._content

        
        @property
        def url(self)-> str:
            """url string"""
            return self._url


        @property
        def reason(self)-> bytes:
            """response error reason"""
            if 400 <= self.status_code:
                return self.content
            else:
                return None


        def json(self)-> Any:
            """response data object"""
            return json.loads(self.text)


        def raise_for_status(self)-> None:
            """Raises :class:`HTTPError`, if one occurred."""
            if 400 <= self.status_code:
                from urllib.error import HTTPError
                reason_text = self._get_reason_text()
                http_error_msg = f'{self.status_code} {"Client" if self.status_code < 500 else "Server"} Error: {reason_text} for url: {self.url}'
                raise HTTPError(self.url, self.status_code, http_error_msg, self.headers, None)

        
        def _get_pagedata_bytes(self, _bytes:bytes)-> str:
            if self.headers.get('Content-Encoding') == 'gzip':
                pagedata = gzip.decompress(_bytes)
            elif self.headers.get('Content-Encoding') == 'deflate':
                pagedata = _bytes
            elif self.headers.get('Content-Encoding'):
                print('Encoding type unknown')
                pagedata = _bytes
            else:
                pagedata = _bytes
            return pagedata


        def _get_reason_text(self)-> str:
            if isinstance(self.reason, bytes):
                # We attempt to decode utf-8 first because some servers
                # choose to localize their reason strings. If the string
                # isn't utf-8, we fall back to iso-8859-1 for all other
                # encodings. (See PR #3538)
                text = self._decode(self.reason)
            else:
                text = self.reason
            return text


        def _decode(self, _bytes: bytes)-> str:
            try:
                text = _bytes.decode('utf-8')
            except UnicodeDecodeError:
                text = _bytes.decode('iso-8859-1')
            return text


class HttpClient(object):
    """Simple http client based on urllib"""

    supported_methods = {'DELETE', 'GET', 'PATCH', 'POST', 'PUT', 'HEAD', 'OPTIONS'}

    def __init__(self, host: str=None, path: str=None, headers: dict=None, timeout: float=None)-> None:

        global requests_module
        self._requests = requests_module
        self._headers: dict = headers or self._default_headers()
        self._timeout: float = timeout
        self._url: str = host
        if path:
            self._url = f"{self._url}/{path}"


    def _default_headers(self)-> dict:
        return {
                # 'User-Agent': f'{Constants.MAGIC_PACKAGE_NAME}/{__version__}',
                # 'Accept-Encoding': 'gzip, deflate',
                # 'Accept': '*/*',
                # 'Connection': 'keep-alive',
            }


    def post(self, url: str, params: Union[dict,str]=None, data: Union[str,bytes,Any]=None, headers: dict=None, json: Any=None, timeout: float=None, **kwargs)-> Response:
        """HTTP post.
        :param string url: target url
        :param dict params: A dict to be url-encoded and sent as query-string.
        :param dict headers: A dict representing headers to be sent via request.
        :param data:
            Implementation needs to support 2 types.
            * An object, which will need to be urlencode() before being sent.
            * (Recommended) A string, which will be sent in request as-is.
        :param json: object, which will need to be urlencode() before being sent.
        It returns an :class:`~Response`-like object.
        Note: In its async counterpart, this method would be defined as async.
        """

        response = self._execute_http_request(url=url, method="POST", params=params, headers=headers, timeout=timeout, data=data, json=json, **kwargs)
        return response


    def get(self, url: str, params: Union[dict,str]=None, headers: dict=None, timeout: float=None, **kwargs)-> Response:
        """HTTP get.
        :param string url: target url
        :param dict params: A dict to be url-encoded and sent as query-string.
        :param dict headers: A dict representing headers to be sent via request.
        It returns an :class:`~Response`-like object.
        Note: In its async counterpart, this method would be defined as async.
        """

        response = self._execute_http_request(url=url, method="GET", params=params, headers=headers, timeout=timeout, **kwargs)
        return response


    def request(self, method: str=None, url: str=None, params: Union[dict,str]=None, headers: dict=None, timeout: float=None, data: Union[str,bytes,Any]=None, json: Any=None, **kwargs)-> Response:
        """HTTP request.
        :param string url: target 
        :param string method: http verb
        :param dict params: A dict to be url-encoded and sent as query-string.
        :param dict headers: A dict representing headers to be sent via request.
        :param data:
            Implementation needs to support 2 types.
            * An object, which will need to be urlencode() before being sent.
            * (Recommended) A string, which will be sent in request as-is.
        :param json: object, which will need to be urlencode() before being sent.
        It returns an :class:`~Response`-like object.
        Note: In its async counterpart, this method would be defined as async.
        """

        response = self._execute_http_request(url=url, method=method, params=params, headers=headers, timeout=timeout, data=data, json=json, **kwargs)
        return response


    def close(self):  # Not required, but we use it to avoid a warning in unit test
        pass


    def _execute_http_request(self, url: str=None, method: str=None, params: Union[dict,str]=None, headers: dict=None, timeout: float=None, data: Union[str,bytes,Any]=None, json: Any=None, **kwargs)-> Response:
        # assert not kwargs, "Our stack shouldn't leak extra kwargs: %s" % kwargs
        try:
            method = self._get_method(method=method, data=data or json)
            headers = self._get_headers(headers=headers, json=json)
            timeout = timeout or self._timeout

            if self._requests:
                url = self._build_url(url=url)
                logger().debug(f"{method} Request: {url}, params: {params}, payload: {data or json}, headers: {headers}")
                data: bytes = self._get_data(data=data)
                response = self._requests.request(method, url, params=params, headers=headers, timeout=timeout, data=data, json=json)
            else:
                import urllib.request
                from urllib.request import Request
                from urllib.error import HTTPError
                ssl_context = None
                if url.lower().startswith("https"):
                    import ssl
                    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
                    # ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    # ssl_context.verify_mode = ssl.CERT_REQUIRED
                    # ssl_context.check_hostname = True
                url = self._build_url(url=url, params=params)
                data: bytes = self._get_data(data=data, json=json)
                request = Request(url, data=data, headers=headers, method=method)
                logger().debug(f"{method} Request: {request.get_full_url()}, payload: {request.data}, headers: {request.headers}")

                try:
                    result = urllib.request.urlopen(request, timeout=timeout, context=ssl_context)
                except HTTPError as err:
                    result = HTTPErrorResponse(err)

                response = Response(url, result)

            logger().debug(f"{method} Response: {response.status_code} {response.content}")
            return response

        except Exception as err:
            code = err.code if hasattr(err, "code") else None
            reason = err.reason if hasattr(err, "reason") else err.read() if hasattr(err, "read") else None
            logger().debug(f"{method} {url} Response: {code} {reason}")
            raise err        


    def _build_url(self, url: str=None, params: Union[dict,str]=None)-> str:
        url = url or self._url
        if params:
            if type(params) == str:
                url_values = params
            elif isinstance(params, dict):
                url_values = urlencode(sorted(params.items()), True)
            else: 
                raise Exception(f"http_client support str|dict params only. type(prams) == {type(params)}")
            url = f"{url}?{url_values}"
        return url


    def _get_method(self, method: str=None, data: Union[str,bytes,Any]=None)-> str:
        method = method or ("GET" if data is None else "POST")
        method = method.upper()
        if method not in self.supported_methods:
            raise Exception(f"method {method} is not supported")
        return method


    def _get_headers(self, headers: dict=None, json: Any=None)-> dict:
        request_headers: dict = self._headers.copy()
        if headers:
            request_headers.update(headers)
            # Remove keys that are set to None.
            none_keys = [k for (k, v) in request_headers.items() if v is None]
            for key in none_keys:
                del request_headers[key]
        if json and 'Content-Type' not in request_headers:
            request_headers['Content-Type'] = 'application/json'
        return request_headers


    def _get_data(self, data: Union[str,bytes,Any]=None, json: Any=None)-> bytes:
        if data is not None:
            if type(data) != bytes:
                if type(data) == str:
                    data = data.encode('utf-8')
                elif type(data) == dict:
                    data = urlencode(data).encode('utf-8')
                else:
                    data = self._json_dumps_encode(data)
        elif json is not None:
            data = self._json_dumps_encode(json)
        return data


    def _json_dumps_encode(self, data: Any)-> bytes:
        data = json.dumps(data, allow_nan=False).encode('utf-8')
        return data
