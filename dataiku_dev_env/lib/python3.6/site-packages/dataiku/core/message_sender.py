from dataiku.core import base, flow
import os.path as osp, os, shutil
import json
from dataiku.core.intercom import backend_json_call

class MessageSender():
    """
    This is a handle to interact with a message sender
    """

    def __init__(self, channel_id, type=None, configuration={}):
        self.type = type
        self.channel_id = channel_id
        self.configuration = configuration
        self.channel = None

    def _repr_html_(self,):
        s = "MessageSender[   <b>%s</b>   ]" % (self.channel_id)
        return s

    def get_channel(self):
        if self.channel is None:
            self.channel = backend_json_call("integration-channels/get", data={
                "id" : self.channel_id
            })
        return self.channel

    def send(self, message, variables={}, project_key=None, **kwargs):
        final_params = self.configuration.copy()
        final_params.update(kwargs)
        final_params['channelId'] = self.channel_id
        final_params['message'] = message

        if project_key is None or len(project_key) == 0:
            project_key = os.environ.get("DKU_CURRENT_PROJECT_KEY", None)

        data = {
            "projectKey": project_key,
            "messaging" : json.dumps({
                "type" : self.type,
                "configuration" : final_params,
            }),
            "variables" : json.dumps(variables)
        }

        return backend_json_call("scenarios/send-message/", data=data, err_msg="Failed to send message")
