import os, json
from dataiku.core.intercom import backend_json_call

def get_webapp_config():
    """Returns a map of the webapp config.
    Parameters are set by the user in in DSS' GUI"""
    return json.loads(os.getenv("DKU_CUSTOM_WEBAPP_CONFIG"))

def get_plugin_config():
    """Returns the global settings of the plugin"""
    return json.loads(os.getenv("DKU_PLUGIN_CONFIG"))

def get_webapp_resource():
    """Returns the path to the folder holding the plugin resources"""
    return os.getenv("DKU_CUSTOM_RESOURCE_FOLDER")
   
def get_plugin_settings(config=None):
    if config is None:
        # get the config from the env var
        config = get_webapp_config()
        if config.get("$isChartDef", None) is not None:
            config = config.get("webAppConfig", {})
    return backend_json_call('plugins/get-resolved-settings', data={'elementConfig':json.dumps(config)})
    