import shutil
import os
import threading
import sys
import requests


class Context():
    "An HTTP Context"

    def __init__(self, cosmian_server_url: str):
        self.url = cosmian_server_url
        self.session = requests.Session()

    def __del__(self):
        self.session.close()

    def delete(self, path: str, params: dict = None, error_message: str = None, allow_404: bool = False) -> dict:
        """
        send a DELETE request to the Cosmian server and return a json payload
        If `allow_404` is set to `True`, the method will return `None` rather than failing,
        when the server returns a status code of 404, 
        """
        return _get_delete(self.session, self.url, path, params, error_message, allow_404, is_delete=True)

    def delete_async(self, path: str, callback, context=None, params: dict = None, error_message: str = None, allow_404: bool = False) -> threading.Thread:
        """
        Send an asynchronous DELETE request on a thread to the Cosmian server.
        and the passed file name to 'data.bin'.
        If `allow_404` is set to `True`, the method will return `None` rather than failing,
        when the server returns a status code of 404, 
        When the thread completes, it will call `callback(error, json, context)` where in case of
         - failure: `error` will be a `ValueError` and `json` will be None
         - success: `error` will be `None` and `json` will contain the json response

        `context` is any thread safe object which is passed through to the callback
        The method returns the thread

        """
        # requests.Session is not thread safe: see https://github.com/psf/requests/issues/2766
        # so we create a new session object
        t = threading.Thread(target=_get_delete_callback, args=(
            None, self.url, path, callback, context, params, error_message, allow_404, True))
        t.daemon = True
        t.start()
        return t

    def get(self, path: str, params: dict = None, error_message: str = None, allow_404: bool = False) -> dict:
        """
        send a GET request to the Cosmian server and return a json response
        If `allow_404` is set to `True`, the method will return `None` rather than failing,
        when the server returns a status code of 404, 
        """
        return _get_delete(self.session, self.url, path, params, error_message, allow_404, is_delete=False)

    def get_async(self, path: str, callback, context=None, params: dict = None, error_message: str = None, allow_404: bool = False) -> threading.Thread:
        """
        Send an asynchronous GET request on a thread to the Cosmian server.
        and the passed file name to 'data.bin'.
        If `allow_404` is set to `True`, the method will return `None` rather than failing,
        when the server returns a status code of 404, 
        When the thread completes, it will call `callback(error, json, context)` where in case of
         - failure: `error` will be a `ValueError` and `json` will be None
         - success: `error` will be `None` and `json` will contain the json response

        `context` is any thread safe object which is passed through to the callback
        The method returns the thread

        """
        # requests.Session is not thread safe: see https://github.com/psf/requests/issues/2766
        # so we create a new session object
        t = threading.Thread(target=_get_delete_callback, args=(
            None, self.url, path, callback, context, params, error_message, allow_404, False))
        t.daemon = True
        t.start()
        return t

    def post(self, path: str, json: dict, params: dict = None, error_message: str = None) -> dict:
        """
        send a POST request with a json payload to the Cosmian server and return a json payload
        """
        return _post_put(self.session, self.url, path, json, params, error_message, is_put=False)

    def post_async(self, path: str, json: dict, callback, context=None, params: dict = None, error_message: str = None) -> threading.Thread:
        """
        Send an asynchronous POST request on a thread to the Cosmian server.
        and the passed file name to 'data.bin'.
        When the thread completes, it will call `callback(error, json, context)` where in case of
         - failure: `error` will be a `ValueError` and `json` will be None
         - success: `error` will be `None` and `json` will contain the json response

        `context` is any thread safe object which is passed through to the callback
        The method returns the thread

        """
        # requests.Session is not thread safe: see https://github.com/psf/requests/issues/2766
        # so we create a new session object
        t = threading.Thread(target=_post_put_callback, args=(
            None, self.url, path, json, callback, context, params, error_message, False))
        t.daemon = True
        t.start()
        return t

    def put(self, path: str, json: dict, params: dict = None, error_message: str = None) -> dict:
        """
        send a PUT request with a json payload to the Cosmian server and return a json payload
        """
        return _post_put(self.session, self.url, path, json, params, error_message, is_put=True)

    def put_async(self, path: str, json: dict, callback, context=None, params: dict = None, error_message: str = None) -> threading.Thread:
        """
        Send an asynchronous PUT request on a thread to the Cosmian server.
        and the passed file name to 'data.bin'.
        When the thread completes, it will call `callback(error, json, context)` where in case of
         - failure: `error` will be a `ValueError` and `json` will be None
         - success: `error` will be `None` and `json` will contain the json response

        `context` is any thread safe object which is passed through to the callback
        The method returns the thread

        """
        # requests.Session is not thread safe: see https://github.com/psf/requests/issues/2766
        # so we create a new session object
        t = threading.Thread(target=_post_put_callback, args=(
            None, self.url, path, json, callback, context, params, error_message, True))
        t.daemon = True
        t.start()
        return t

    def download(self, path: str, data_file: str, params: dict = None, json: dict = None,
                 error_message: str = None) -> int:
        """
        Trigger a download to the given data_file.
        This method will issue a GET request if the 'json' parameter is None,
        a POST otherwise
        Returns the number of bytes downloaded.
        """
        return _download(self.session, self.url, path,
                         data_file, params, json, error_message)

    def download_async(self, path: str, data_file: str, callback, context=None,
                       params: dict = None, json: dict = None, error_message: str = None) -> threading.Thread:
        """
        Trigger a download to the given data_file.
        This method will issue a GET request if the 'json' parameter is None,
        a POST otherwise
        When the thread completes, it will call `callback(error, size, context)` where in case of
         - failure: `error` will be a `ValueError` and `size` will be None
         - success: `error` will be `None` and `size` will contain the number bytes downloaded

        `context` is any thread safe object which is passed through to the callback
        The method returns the thread

        """
        # requests.Session is not thread safe: see https://github.com/psf/requests/issues/2766
        # so we create a new session object
        t = threading.Thread(target=_download_callback, args=(
            None, self.url, path, data_file, callback, context, params, json, error_message))
        t.daemon = True
        t.start()
        return t

    def upload(self, path: str, data_file: str,
               content_type='application/octet-stream', file_name='data.bin', params: dict = None,
               error_message: str = None
               ) -> dict:
        """
        send a POST request to the Cosmian server to upload the given file
        with the specified content-type and file name.
        The content-type defaults to 'application/octet-stream'
        and the passed file name to 'data.bin'.
        Returns the json response.
        """
        return _upload(self.session, self.url, path, data_file, content_type, file_name, params, error_message)

    def upload_async(self, path: str, data_file: str, callback, context=None,
                     content_type='application/octet-stream', file_name='data.bin', params: dict = None,
                     error_message: str = None
                     ) -> threading.Thread:
        """
        Send an asynchronous POST request on a thread to the Cosmian server to upload the given file
        with the specified content-type and file name.
        The content-type defaults to 'application/octet-stream'
        and the passed file name to 'data.bin'.
        When the thread completes, it will call `callback(error, json, context)` where in case of
         - failure: `error` will be a `ValueError` and `json` will be None
         - success: `error` will be `None` and `json` will contain the json response

        `context` is any thread safe object which is passed through to the callback
        The method returns the thread
        """
        # requests.Session is not thread safe: see https://github.com/psf/requests/issues/2766
        # so we create a new session object
        t = threading.Thread(target=_upload_callback, args=(
            None, self.url, path, data_file, callback, context, content_type, file_name, params, error_message))
        t.daemon = True
        t.start()
        return t


def _get_delete(session: requests.Session, server_url: str, path: str, params: dict = None,
                error_message: str = None, allow_404: bool = False, is_delete: bool = False) -> dict:
    """
    send a GET or DELETE request to the Cosmian server and return a json payload
    If the session is None, one is created
    """
    if session is None:
        http = requests.Session()
    else:
        http = session
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    if is_delete:
        headers["x-http-method-override"] = "DELETE"
    try:
        r = http.get(
            url="%s%s" % (server_url, path),
            params=params,
            headers=headers
        )
        if allow_404 and r.status_code == 404:
            return None
        if r.status_code >= 400:
            if error_message is None:
                err = "failed getting %s: " % path
            else:
                err = error_message
            raise ValueError("%s, status code: %s, reason: %s" % (
                err, r.status_code, r.text))
        return r.json()
    except requests.ConnectionError as e:
        raise ValueError(
            "Failed querying Cosmian Server at: %s, error: %s" % (server_url, e))
    finally:
        if session is None:
            http.close()


def _get_delete_callback(session: requests.Session, server_url: str, path: str, callback, context=None,
                         params: dict = None, error_message: str = None, allow_404: bool = False, is_delete: bool = False) -> None:
    try:
        result = _get_delete(session, server_url, path,
                             params, error_message, allow_404, is_delete)
        callback(None, result, context)
    except:
        callback(sys.exc_info()[1], None, context)


def _post_put(session: requests.Session, server_url: str, path: str, json: dict, params: dict = None,
              error_message: str = None, is_put: bool = False) -> dict:
    """
    send a POST or PUT request with a json payload to the Cosmian server and return a json payload
    If the session is None, one is created
    """
    if session is None:
        http = requests.Session()
    else:
        http = session
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
        "Content-type": "application/json"
    }
    if is_put:
        headers["x-http-method-override"] = "PUT"
    try:
        r = http.post(
            url="%s%s" % (server_url, path),
            params=params,
            json=json,
            headers=headers
        )
        if r.status_code >= 400:
            if error_message is None:
                err = "failed posting to %s: " % path
            else:
                err = error_message
            raise ValueError("%s, status code: %s, reason: %s" % (
                err, r.status_code, r.text))
        return r.json()
    except requests.ConnectionError as e:
        raise ValueError(
            "Failed querying Cosmian Server at: %s, error: %s" % (server_url, e))
    finally:
        if session is None:
            http.close()


def _post_put_callback(session: requests.Session, server_url: str, path: str, json: dict, callback, context=None,
                       params: dict = None, error_message: str = None, is_put: bool = False) -> None:
    try:
        result = _post_put(session, server_url, path,
                           json, params, error_message, is_put)
        callback(None, result, context)
    except:
        callback(sys.exc_info()[1], None, context)


def _download(session: requests.Session, server_url: str, path: str, data_file: str, params: dict = None, json: dict = None,
              error_message: str = None) -> int:
    """
    Trigger a download to the given data_file.
    This method will issue a GET request if the 'json' parameter is None,
    a POST otherwise
    Returns the number of bytes downloaded.
    If the session is None, one is created
    """
    if session is None:
        http = requests.Session()
    else:
        http = session
    try:
        if json is None:
            r = http.get(
                url="%s%s" % (server_url, path),
                params=params,
                stream=True
            )
        else:
            r = http.post(
                url="%s%s" % (server_url, path),
                params=params,
                json=json,
                stream=True
            )
        with r:
            if r.status_code >= 400:
                if error_message is None:
                    err = "failed downloading the data from: %s" % path
                else:
                    err = error_message
                raise ValueError("%s, status code: %s, reason: %s" % (
                    err, r.status_code, r.text))
            with open(data_file, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            return os.stat(data_file).st_size
    except requests.ConnectionError as e:
        raise ValueError(
            "Failed querying Cosmian Server at: %s, error: %s" % (server_url, e))
    finally:
        if session is None:
            http.close()


def _download_callback(session: requests.Session, server_url: str, path: str, data_file: str, callback, context=None,
                       params: dict = None, json: dict = None, error_message: str = None) -> None:
    try:
        result = _download(session, server_url, path, data_file,
                           params, json, error_message)
        callback(None, result, context)
    except:
        callback(sys.exc_info()[1], None, context)


def _upload(session: requests.Session, server_url: str, path: str, data_file: str,
            content_type='application/octet-stream', file_name='data.bin', params: dict = None,
            error_message: str = None
            ) -> dict:
    """
    send a POST request to the Cosmian server to upload the given file
    with the specified content-type and file name.
    The content-type defaults to 'application/octet-stream'
    and the passed file name to 'data.bin'.
    Returns the json response.
    """
    with open(data_file, 'rb') as data_file:
        files = {'file': (file_name, data_file,
                          content_type, {'Expires': '0'})}
        if session is None:
            http = requests.Session()
        else:
            http = session
        try:
            r = http.post(
                url="%s%s" % (server_url, path),
                files=files,
                params=params
            )
            if r.status_code >= 400:
                if error_message is None:
                    err = "failed uploading the data to: %s" % path
                else:
                    err = error_message
                raise ValueError("%s, status code: %s, reason: %s" % (
                    err, r.status_code, r.text))
            return r.json()
        except requests.ConnectionError as e:
            raise ValueError(
                "Failed querying Cosmian Server at: %s, error: %s" % (server_url, e))
        finally:
            if session is None:
                http.close()


def _upload_callback(session: requests.Session, server_url: str, path: str, data_file: str, callback, context=None,
                     content_type='application/octet-stream', file_name='data.bin', params: dict = None,
                     error_message: str = None
                     ) -> None:
    try:
        result = _upload(session, server_url, path, data_file,
                         content_type, file_name, params, error_message)
        callback(None, result, context)
    except:
        callback(sys.exc_info()[1], None, context)
