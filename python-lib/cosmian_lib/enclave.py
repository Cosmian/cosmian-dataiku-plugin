from urllib.parse import urlparse
from .server import Server
from .context import Context


class Enclave():

    def __init__(self, context: Context):
        self.context = context

    def deploy_python_code(self, remote_server_url, algo_name, python_code):
        # FIXME: later on we want to pass the remote server url directly without parsing
        parsed_remote = urlparse(remote_server_url)
        if parsed_remote.netloc == '':
            raise ValueError('Invalid remote server url: %s' %
                             remote_server_url)
        data = {
            'hostname': parsed_remote.netloc,
            'algo_name': algo_name,
            'code': python_code
        }
        return self.context.post("enclave/code_update", data,
                                 error_message="enclave:: failed deploying protected code to: %s" % remote_server_url
                                 )["success"]

    def run_protected_algorithm(self, views, algo_name, output_name):
        # first delete any existing output view
        Server.from_context(self.context).views().delete_view(output_name)
        headers = {
            "Accept-Encoding": "gzip",
            "Accept": "application/json",
            "Content-type": "application/json"
        }
        data = {
            'server_url': self.context.url,
            'views': views,
            'output_name': output_name
        }
        return self.context.post("enclave/run_code/%s" % algo_name, data,
                                 error_message="enclave:: failed running algorithm: %s" % algo_name
                                 )
