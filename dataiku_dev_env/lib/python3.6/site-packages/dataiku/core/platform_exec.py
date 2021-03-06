from __future__ import print_function
import os
import signal
import subprocess
import json
import threading
import pipes
import time, logging, traceback, sys
from contextlib import contextmanager
from collections import deque
from dataiku.base import remoterun

DKUBIN = None
def _get_dku_bin():
    assert remoterun.has_env_var("DKUBIN"), "DKUBIN environment variable must be set."
    DKUBIN = remoterun.get_env_var("DKUBIN")
    return DKUBIN

class DKUProcessException(Exception):
    def __init__(self, msg, arguments, return_code, stderr, cause=None):
        Exception.__init__(self, msg)
        self.arguments = arguments
        self.return_code = return_code
        self.stderr = stderr
        self.cause = cause


def _run_dku(args, verbose=False, dip_home=None, pipe_stdin=subprocess.PIPE, add_env=None):
    pipe_stdin = pipe_stdin or subprocess.PIPE  # DEPRECATED, default used to be False
    cmd_str = " ".join([pipes.quote(arg) for arg in [_get_dku_bin()] + args])
    child_env = remoterun.get_env_vars().copy()
    if dip_home is not None:
        child_env["DIP_HOME"] = dip_home
    if add_env is not None:
        child_env.update(add_env)
    if verbose:
        print("Running", cmd_str)
        print("With env", json.dumps(child_env))
    return subprocess.Popen(cmd_str,
                            stdin=pipe_stdin,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            preexec_fn=os.setsid,
                            env=child_env)


@contextmanager
def open_dku_stream(args, verbose=False, dip_home=None, add_env={}):
    """
    Context manager to spawn a DKU process and
    reads its output as a stream

    It is used as follows :

        with read_dku([cmdline arguments]) as dku_output:
            # your code using dku_output

    where DKU is the process stdout.

    If the process returns with a return_code
    different than -1, a DKUProcessException will
    be thrown.

    dip_home -- Absolute path to the DIP DIP_HOME
    """
    logging.info("Running DKU with %s" % args)
    process = _run_dku(args, verbose, dip_home, add_env=add_env)
    stderr_chunks = deque(maxlen=100)

    # we need to have a thread read stderr to make sure that
    # the child process does not block when OS buffer is full.
    def read_stderr():
        while True:
            # Read should be nice enough to block.
            data = process.stderr.read()
            if not data:
                # child process closed stderr
                return
            stderr_chunks.append(data)
    stderr_thread = threading.Thread(target=read_stderr)
    # our process shouldn't wait for this thread to terminate.
    stderr_thread.daemon = True
    stderr_thread.start()
    error = None
    error_details = None
    exited_regularly = False
    try:
        yield process.stdout
        exited_regularly = True
    except Exception as e:
        error = e
        error_details = sys.exc_info()
    except GeneratorExit as e:
        # generator exit is considered regular.
        # it happens for with the timeout exception.
        exited_regularly = True
    finally:
        #
        # Either something bad happened while reading the pipe,
        # or we finished reading DKU's stdout.
        #
        # Let's check for it terminate and check for some
        # possible errors.
        #
        # Give 10s to the process to put sensible stuff on the stderr
        # terminate and close it.
        #
        # Is the process finished
        return_code = process.poll()
        logging.info("Returned with %s" % return_code)
        if return_code is None:
            # the process is still running.
            # this happens when someone does not consume the process entirely before
            # releasing its stdout.
            #
            # Either explicitly, or because he reached a timeout.
            try:
                process.stdout.close()
            except:
                pass
            return_code = process.poll()
            if return_code is None:
                pass
            # wait 1sec for the program to terminate
            time.sleep(1)
            return_code = process.poll()
            if return_code is None:
                # still not terminated ? kill
                # the subprocess and all of its children.
                os.killpg(process.pid, signal.SIGTERM)

        stderr_thread.join(2)
        stderr_join = "".join(stderr_chunks)

        if exited_regularly and return_code == 0:
            logging.info("Exit OK")
            return
        msg = []
        if return_code:
            msg.append("+ DKU Process did not end properly (retcode=%s)" % return_code)
        if not exited_regularly:
            msg.append("+ Reading DKU Process encountered the following Exception.")
            msg.append(str(error.__class__.__name__) + ": " + str(error))
            if error_details is not None:
                msg.append("Stack")
                msg.append(traceback.format_exc(error_details))
            else:
                msg.append("No stack available ?")
        msg.append("Command was :")
        msg.append("  " + str(args))
        if return_code is not None:
            msg.append("Return code %i" % return_code)
        else:
            msg.append("Had to kill the process.")
        msg.append("Std err %s" % stderr_join)
        raise DKUProcessException("\n".join(msg), args, return_code, stderr_join, cause=error)



def read_dku_json(args, verbose=False, dip_home=None):
    """
    Spawns a dku process and
    read its stdout as a json.

    Throws a DKUProcessException if the process fails.
    Throws a ValueError if the json returns is incorrect.
    """
    with open_dku_stream(args, verbose, dip_home) as dku_output:
        return json.load(dku_output)
