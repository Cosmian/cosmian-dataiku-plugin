from .context import Context


class Enclave():
    """
    Sender is the Algo provider
    Receiver is the Data provider
    """

    def __init__(self, context: Context):
        self.context = context

    def handshake(self, remote_data_provider):
        """
        Initial handshake for code deployment.
        This call initializes cryptographic primtives between algo provider and data provider.
        """
        data = {
            'remote_data_provider': remote_data_provider
        }
        return self.context.post("/enclave/sender/handshake", data,
                                 error_message="enclave:: failed initial handshake with: %s" % remote_data_provider
                                 )

    def remote_attestation(self, quote):
        """
        Performs remote attestation procedure with remote Intel IAS Servers.
        Result of handshake procedure is used to verify the integrity of data provider's SGX platform.
        Though this step is optional, it is recommended to call it.
        """
        return self.context.post("/enclave/sender/remote_attestation", quote,
                                 error_message="enclave:: failed remote attestation"
                                 )

    def encrypt_code(self, python_code):
        """
        Encrypt given code using cryptographic primitives set up during handshake.
        """
        data = {
            'code': python_code
        }
        return self.context.post("/enclave/sender/encrypt", data,
                                 error_message="enclave:: failed encrypting code"
                                 )["enc_code"]

    def send_python_code(self, remote_server_url, algo_name, python_code):
        """
        Send encrypted code to data provider.
        """
        data = {
            'remote_data_provider': remote_server_url,
            'enc_code': python_code
        }
        return self.context.post("/enclave/sender/code/%s" % algo_name, data,
                                 error_message="enclave:: failed deploying code to: %s" % remote_server_url
                                 )["success"]

    def run_code(self, algo_name, params=None):
        """
        Run code given its `algo_name` and its `params` (optional).
        """
        data = {
            'params': params,
        }
        return self.context.post("/enclave/receiver/code/%s" % algo_name, data,
                                 error_message="enclave:: failed running algorithm: %s" % algo_name
                                 )

    def list_encrypted_codes(self):
        """
        Get a list of deployed algorithms.
        """
        return self.context.get("/enclave/receiver/codes", None,
                                error_message="enclave:: failed getting algorithms list: %s"
                                )

    def show_encrypted_code(self, algo_name):
        """
        Get an hexadecimal representation of deployed algorithm given its `algo_name`.
        """
        return self.context.get("/enclave/receiver/code/%s" % algo_name, None,
                                error_message="enclave:: failed showing algorithm: %s" % algo_name
                                )

    def delete_code(self, algo_name):
        """
        Delete a deployed algorithm given its `algo_name`.
        """
        return self.context.delete("/enclave/receiver/code/%s" % algo_name, None,
                                   error_message="enclave:: failed deleting algorithm: %s" % algo_name
                                   )

    def push_data(self, filename, content):
        """
        Push some data content to data provider.
        """
        data = {
            'data': content
        }
        return self.context.post("/enclave/receiver/data/%s" % filename, data,
                                 error_message="enclave:: failed pushing data as %s" % filename
                                 )["success"]

    def delete_data(self, filename):
        """
        Delete previously sent data to data provider, given its `filename`.
        """
        return self.context.delete("/enclave/receiver/data/%s" % filename, None,
                                   error_message="enclave:: failed deleting data as %s" % filename
                                   )
