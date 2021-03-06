import os, json
from dataiku.core import flow

def get_input_names(full=True):
    flow_spec = flow.FLOW
    return [x[full and "fullName" or "smartName"] for x in flow_spec["in"]]

def get_output_names(full=True):
    flow_spec = flow.FLOW
    return [x[full and "fullName" or "smartName"] for x in flow_spec["out"]]

def get_input_names_for_role(role, full=True):
    flow_spec = flow.FLOW
    return [x[full and "fullName" or "smartName"] for x in flow_spec["in"] if x["role"] == role]

def get_output_names_for_role(role, full=True):
    flow_spec = flow.FLOW
    return [x[full and "fullName" or "smartName"] for x in flow_spec["out"] if x["role"] == role]


def get_recipe_config():
    """Returns a map of the recipe parameters.
    Parameters are defined in recipe.json (see inline doc in this file)
    and set by the user in the recipe page in DSS' GUI"""
    return json.loads(os.getenv("DKU_CUSTOM_RECIPE_CONFIG"))
    
def get_plugin_config():
    """Returns the global settings of the plugin"""
    return json.loads(os.getenv("DKU_CUSTOM_RECIPE_PLUGIN_CONFIG"))

def get_recipe_resource():
    """See get_connector_resource() in the custom dataset API."""
    return os.getenv("DKU_CUSTOM_RESOURCE_FOLDER")
