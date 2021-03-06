import threading
from .context import Context


class PSI():

    def __init__(self, context: Context):
        self.context = context

    def clean(self, uid) -> str:
        """
        Clean all generated data for the given uid
        """
        return self.context.delete("/psi/%s" % uid, None,
                                   "PSI:: failed cleaning up data files for uid: %s" % uid
                                   )["success"]

    def clean_all(self) -> str:
        """
        Clean all generated data files
        """
        return self.context.delete("/psi/all", None,
                                   "PSI: : failed cleaning up all data files",
                                   )["success"]

    def receiver_setup_from_url(self, url) -> str:
        """
        Set-up receiver data with values read from an url (one hex string per line)
        Returns the uid.
        """
        return self.context.get("/psi/receiver/setup", {"url": url},
                                "PSI:: failed setting up a receiver with data from an url"
                                )["uuid"]

    def receiver_setup_from_view(self, view_name: str, column_name: str = None) -> str:
        """
        Set-up receiver data with values from a view and return the uid
        """
        params = {
            "view": view_name,
        }
        if column_name is not None:
            params.column = column_name
        return self.context.get("/psi/receiver/setup", params,
                                "PSI:: failed setting up a receiver with data from a view"
                                )["uuid"]

    def receiver_setup_from_hex_strings_array(self, hex_string_array) -> str:
        """
        Set-up receiver data with values from an array of hex strings and return the uid
        """
        return self.context.post("/psi/receiver/setup/post", hex_string_array, None,
                                 "PSI:: Error setting up a receiver with data from hex strings"
                                 )["uuid"]

    def receiver_download_sender_data(self, uid: str, receiver_data_file: str) -> int:
        """
        Download to the given receiver_data_file,
        the receiver data generated during the receiver set-up for the given uid.
        Returns the number of bytes downloaded.
        """
        return self.context.download("/psi/receiver/data/%s" % uid, receiver_data_file, None, None,
                                     "PSI:: failed downloading the sender data for uid: %s" % uid
                                     )

    def sender_setup_sync(self, receiver_data_file: str) -> str:
        """
        Set-up sender by uploading a receiver data file, returns the uid.
        This is a synchronous call and may take a few minutes to return
        """
        return self.context.upload("/psi/sender/setup", receiver_data_file,
                                   error_message="PSI: : Error setting up a sender from: %s" % receiver_data_file
                                   )["uuid"]

    def sender_setup_async(self, receiver_data_file: str, callback, context=None) -> threading.Thread:
        """
        Set-up sender by uploading a receiver data file, returns the uid.
        This is an asynchronous call which will immediately return with the thread object.
        When the thread completes, it will call `callback(error, uid, context)` where in case of
         - failure: `error` will be a `ValueError` and `uid` will be None
         - success: `error` will be `None` and `uid` will contain a string value

        `context` is any thread safe object which is passed through to the callback
        """
        return self.context.upload_async("/psi/sender/setup", receiver_data_file, callback, context,
                                         error_message="PSI:: failed to asynchronously setup the sender with data file: %s" % receiver_data_file
                                         )

    def sender_process_from_url_sync(self, uid: str, results_file: str, url: str) -> int:
        """
        Synchronously process the intersection of receiver data setup for the given uid
        against values read from an url (one hex string per line).
        The encrypted results will be downloaded to the given `results_file`
        Returns the number of bytes downloaded.
        """
        return self.context.download("/psi/sender/process/%s" %
                                     uid, results_file, {"url": url}, None,
                                     "PSI:: failed processing the sender intersection for the uid: %s and the url: %s" % (
                                         uid, url)
                                     )

    def sender_process_from_view_sync(self, uid, results_file: str, view_name: str, column_name: str = None) -> int:
        """
        Synchronously process the intersection of receiver data setup for the given uid
        against values from a view. If the column name is not specified, the first column will be chosen.
        The encrypted results will be downloaded to the given `results_file`
        Returns the number of bytes downloaded.
        """
        params = {
            "view": view_name,
        }
        if column_name is not None:
            params.column = column_name
        return self.context.download("/psi/sender/process/%s" %
                                     uid, results_file, params, None,
                                     "PSI:: failed processing the sender intersection for the uid: %s and the view: %s" % (
                                         uid, view_name)
                                     )

    def sender_process_from_hex_strings_array_sync(self, uid: str, results_file: str, hex_strings_array) -> int:
        """
        Synchronously process the intersection of receiver data setup for the given uid
        against values from an array of hex strings.
        The encrypted results will be downloaded to the given `results_file`
        Returns the number of bytes downloaded.
        """
        return self.context.download("/psi/sender/process/post/%s" % uid,
                                     results_file, None, hex_strings_array,
                                     "PSI: : failed processing the sender intersection for the uid: %s" % uid
                                     )

    def sender_process_from_url_async(self, uid: str, results_file: str, callback, context, url: str) -> threading.Thread:
        """
        Asynchronously process the intersection of receiver data setup for the given uid
        against values read from an url (one hex string per line).
        The encrypted results will be saved to the given `results_file`
        When the thread completes, it will call `callback(error, results_file, context)` where in case of
         - failure: `error` will be a `ValueError` and `size` will be None
         - success: `error` will be `None` and `size` will contain the number bytes downloaded

        `context` is any thread safe object which is passed through to the callback
        """
        return self.context.download_async("/psi/sender/process/%s" % uid, results_file, callback, context,
                                           params={"url": url}, json=None,
                                           error_message="PSI:: failed processing the sender intersection for the uid: %s" % uid
                                           )

    def sender_process_from_view_async(self, uid: str, results_file: str, callback, context, view_name: str, column_name: str = None) -> threading.Thread:
        """
        Asynchronously process the intersection of receiver data setup for the given uid
        against values from a view. If the column name is not specified, the first column will be chosen.
        The encrypted results will be saved to the given `results_file`
        When the thread completes, it will call `callback(error, results_file, context)` where in case of
         - failure: `error` will be a `ValueError` and `size` will be None
         - success: `error` will be `None` and `size` will contain the number bytes downloaded

        `context` is any thread safe object which is passed through to the callback
        """
        params = {
            "view": view_name,
        }
        if column_name is not None:
            params.column = column_name
        return self.context.download_async("/psi/sender/process/%s" % uid, results_file, callback, context,
                                           params=params, json=None,
                                           error_message="PSI:: failed processing the sender intersection for the uid: %s" % uid
                                           )

    def sender_process_from_hex_strings_array_async(self, uid: str, results_file: str, callback, context, hex_strings_array) -> threading.Thread:
        """
        Asynchronously process the intersection of receiver data setup for the given uid
        against values from an array of hex strings.
        The encrypted results will be saved to the given `results_file`
        When the thread completes, it will call `callback(error, results_file, context)` where in case of
         - failure: `error` will be a `ValueError` and `size` will be None
         - success: `error` will be `None` and `size` will contain the number bytes downloaded

        `context` is any thread safe object which is passed through to the callback
        """
        return self.context.download_async("/psi/sender/process/post/%s" % uid, results_file, callback, context,
                                           params=None, json=hex_strings_array,
                                           error_message="PSI:: failed processing the sender intersection for the uid: %s" % uid
                                           )

    def receiver_reveal_intersection(self, results_file: str) -> tuple:
        """
        Process a sender results file and reveal the intersection for the given uid.
        The results is returned as an array of hex strings
        """
        return list(self.context.upload("/psi/receiver/process", results_file,
                                        error_message="PSI:: failed processing the results file: %s" % results_file
                                        ))
