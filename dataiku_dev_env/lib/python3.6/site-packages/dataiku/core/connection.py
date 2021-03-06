from dataiku.core import intercom
import sys, imp

def get_connection(connection_name):
    #Kernel Servlet does not implement /tintercom/connections/get-details, forcing backend
    conn_obj = intercom.backend_json_call("connections/get-details", data={
        "connectionName" : connection_name
    })
    return DSSConnection(conn_obj)

class DSSConnection:
    """This is a handle to get details of a Dataiku connection"""

    def __init__(self, conn_details):
        self.conn_details = conn_details

    def get_params(self):
        return self.conn_details["params"]
