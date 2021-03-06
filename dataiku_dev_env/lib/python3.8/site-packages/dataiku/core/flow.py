from dataiku.base.remoterun import get_dkuflow_spec

FLOW = None
def load_flow_spec():
    global FLOW
    FLOW = get_dkuflow_spec()

load_flow_spec()