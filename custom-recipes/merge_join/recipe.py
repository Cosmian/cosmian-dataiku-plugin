# Code for custom code recipe merge_join (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# import the classes for accessing DSS objects from the recipe
# import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *
import logging

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
# input_A_names = get_input_names_for_role('input_A_role')
# The dataset objects themselves can then be created like this:
# input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# For outputs, the process is the same:
# output_A_names = get_output_names_for_role('main_output')
# output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]


# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
# my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:
# my_variable = get_recipe_config().get('parameter_name', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import requests
import cosmian

# import pandas as pd, numpy as np
# from dataiku import pandasutils as pdu

logging.warn("****RECIPE RESOURCE: %s", get_recipe_resource())

# an HTTP 1.1 session with keep-alive
session = requests.Session()

# Fetch information for the left dataset
left_dataset = dataiku.Dataset(get_input_names_for_role('left')[0])
# metadata = left_dataset.read_metadata()
# logging.warn("******** METADATA %s", metadata)
# logging.warn("******** CONFIG %s", left_dataset.get_config())
left_config = left_dataset.get_config()['params']['customConfig']
logging.debug("******** Cosmian: LEFT COSMIAN SERVER CONFIG: %s", left_config)
left_view = left_config['view_name']
left_sorted = left_config['sorted']
left_server_url = left_config['server_url']
if not left_server_url.endswith("/"):
    left_server_url += "/"

# Fetch information for the right dataset
right_dataset = dataiku.Dataset(get_input_names_for_role('right')[0])
right_config = right_dataset.get_config()['params']['customConfig']
logging.debug("******** Cosmian: RIGHT COSMIAN SERVER CONFIG: %s", right_config)
right_view = right_config['view_name']
right_sorted = right_config['sorted']
right_server_url = right_config['server_url']
if not right_server_url.endswith("/"):
    right_server_url += "/"

# The two dataset views must reside on the same Cosmian server
if left_server_url != right_server_url:
    raise ValueError("The two datasets must be accessed from the same Cosmian server")
server_url = left_server_url

# Join parameters
join_type = get_recipe_config()['join_type']
outer_table_index = get_recipe_config()['outer_table_index']
num_tables = 2  # FIXME hard-coded upper bound
if outer_table_index < 0 or outer_table_index > num_tables:
    raise ValueError(
        "Invalid outer table index, it must be between 1 and " + str(num_tables)
    )
# For MCFE joins, retrieve the inner join key
join_key = get_recipe_config()['join_key']

# REST request to inner join
handle = cosmian.get_inner_join_handle(
    session, server_url, left_view, right_view,
    join_type, outer_table_index, join_key)

output_dataset = dataiku.Dataset(get_output_names_for_role('output')[0])
output_schema = cosmian.get_schema(session, server_url, handle)
logging.info("******** Cosmian: HANDLE: %s, SCHEMA: %s", handle, output_schema)
output_dataset.write_schema(output_schema)

# Stream entries and write them to the output
with output_dataset.get_writer() as writer:
    while True:
        row = cosmian.read_next_row(session, server_url, handle)
        if row is None:
            break
        writer.write_row_dict(row)
