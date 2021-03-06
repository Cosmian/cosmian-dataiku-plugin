import os, json
from dataiku.core import base
from  dataiku.core.intercom import jek_void_call
from dataiku.core import flow

class PigExecutor:
    def __init__(self):
        pass

    @staticmethod
    def exec_recipe_fragment(script,
                                    overwrite_output_schema=True):
        """Executes a Pig script given as a script as a Pig recipe would"""
        spec = flow.FLOW
        jek_void_call("intercom/pig/execute-partial-recipe",
            data={
                "activityId" : spec["currentActivityId"],
                "script" : script,
                "overwriteOutputSchema" : overwrite_output_schema
            }, err_msg="Query failed")

