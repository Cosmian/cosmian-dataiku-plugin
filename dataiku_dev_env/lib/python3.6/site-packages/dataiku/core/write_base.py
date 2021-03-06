import sys, json, time, threading
import logging
import csv

try:
    import Queue
except:
    import queue as Queue

from dataiku.core import dku_pandas_csv, flow, base, schema_handling, dkuio
import os
from datetime import datetime
from dataiku.core.intercom import jek_or_backend_json_call, jek_or_backend_void_call, jek_or_backend_get_call

if sys.version_info > (3,0):
    from io import StringIO
elif sys.version_info<(2,7,6):
    # Python < 2.7.6 doesn't support writing a bytearray in a cStringIO
    from StringIO import StringIO
else:
    from cStringIO import StringIO


class TimeoutExpired(Exception):
    pass


class TimeoutableQueue(Queue.Queue):
    def __init__(self,size):
        Queue.Queue.__init__(self,size)

    # Return when :
    # - The queue is empty
    # - The timeout expired (without raising!)
    def join_with_timeout(self, timeout):
        self.all_tasks_done.acquire()
        try:
            endtime = time.time() + timeout
            while self.unfinished_tasks:
                remaining = endtime - time.time()
                if remaining <= 0.0:
                    raise TimeoutExpired
                self.all_tasks_done.wait(remaining)
        finally:
            self.all_tasks_done.release()

try:
    from io import BytesIO
except:
    pass


# Send data over HTTP using chunked encoding.
class RemoteStreamWriterBase(threading.Thread):

    def __init__(self, session_id, waiter):
        logging.info("Init RSW")
        self.id = session_id
        self.error_message = None
        self.waiter = waiter
        self.chunk_queue_size = 10
        self.chunk_size = 5000000 # 5MN seems to be the best (both 1MB & 10MB are slower)
        self.queue = TimeoutableQueue(self.chunk_queue_size)

        if sys.version_info > (3,0):
            self.buffer = BytesIO()
        else:
            self.buffer = StringIO()
        self.end_mark = self
        threading.Thread.__init__(self)
        self.daemon = True
        logging.info("Starting RemoteStreamWriter (id=%s)" % (id(self)))

    def _check_health(self):
        if self.error_message:
            raise Exception(self.error_message)
        if not self.queue:
            raise Exception("Stream has been closed")

    def read(self):
        raise Exception("Don't call me baby")

    def flush(self):
        self._check_health()
        if self.buffer.tell()>0:
            self.queue.put(self.buffer.getvalue())
            if sys.version_info > (3,0):
                self.buffer = BytesIO()
            else:
                self.buffer = StringIO()

        while True:
            q = self.queue
            if not q:
                break
            try:
                q.join_with_timeout(1000)
                break
            except TimeoutExpired:
                continue

        if self.error_message:
            raise Exception(self.error_message)

    def write(self, data):
        # logging.info("Remote Stream Writer writes: %s" % data)
        self._check_health()
        self.buffer.write(data)
        if self.buffer.tell() > self.chunk_size:
            self.flush()

    def close(self):
        logging.info("Remote Stream Writer closed")
        self._check_health()
        self.queue.put(self.end_mark)
        self.flush()
        if self.error_message:
            raise Exception(self.error_message)

    def _generate(self):
        logging.info("Remote Stream Writer: start generate")
        while True:
            if not self.waiter.is_still_alive():
                logging.info("Write session has been interrupted")
                return
            logging.info("Waiting for data to send ...")
            try:
                item = self.queue.get(True,10)
            except Queue.Empty:
                # no, no ! empty chunks are forbidden by the HTTP spec  !
                #yield ''
                logging.info("No data to send, waiting more...")
                continue
            if item is self.end_mark:
                logging.info("Got end mark, ending send")
                break
            else:
                logging.info("Sending data (%s)" % len(item))
                yield item
                self.queue.task_done()

    def run(self):
        try:
            logging.info("Initializing write data stream (%s)" % self.id)
            self.streaming_api.push_data_from_generator(self.id,self._generate())
            self.queue.task_done()
        except Exception as e:
            logging.exception("RemoteStreamWriter thread failed")
            self.error_message = 'Error : %s'%e
        finally:
            self.queue = None


MISSING_ID_MARKER = '__not_started__'

# Create a thread which is waiting for the streaming session to complete.
class WriteSessionWaiter(threading.Thread):
    def __init__(self,session_id, session_init_message, api):
        self.session_id = session_id
        self.session_init_message = session_init_message
        self.exception_type = None
        self.alive = True
        self.streaming_api = api
        threading.Thread.__init__(self)
        self.daemon = True
        self.start()

    def raise_on_failure(self):
        if self.exception_type is not None:
            if (sys.version_info > (3, 0)):
                raise self.exception
            else:
                exec("raise self.exception_type, self.exception, self.traceback")

    def is_still_alive(self):
        return self.alive

    def wait_end(self):
        self.join()
        self.raise_on_failure()

    def run(self):
        try:
            if self.session_id == MISSING_ID_MARKER and self.session_init_message is not None:
                raise Exception(u'An error occurred while starting the writing to the dataset : %s' % self.session_init_message)
            self.streaming_api.wait_write_session(self.session_id)
        except Exception as e:
            logging.exception("Exception caught while writing")
            self.exception_type, self.exception, self.traceback = sys.exc_info()
        finally:
            self.alive = False