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
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *
import logging
from cosmian import cosmian
import requests

logging.warn("****RECIPE RESOURCE: %s", get_recipe_resource())

# an HTTP 1.1 session with keep-alive
session = requests.Session()

dataset_names = get_input_names_for_role('datasets')
datasets = [dataiku.Dataset(name) for name in dataset_names]
url, views = cosmian.recover_datasets_info(datasets)

# Join parameters
recipe_config = get_recipe_config()
join_type = recipe_config['join_type']
# For MCFE joins, retrieve the inner join key
if 'compute_key' in recipe_config:
    compute_key = recipe_config['compute_key']
else:
    compute_key = ''

# REST request to inner join
handle = cosmian.get_join_handle(
    session, url, views,
    join_type, compute_key)

output_dataset = dataiku.Dataset(get_output_names_for_role('output')[0])
output_schema = cosmian.get_schema(session, url, handle)
logging.info("******** Cosmian: HANDLE: %s, SCHEMA: %s", handle, output_schema)
output_dataset.write_schema(output_schema)

# Stream entries and write them to the output
with output_dataset.get_writer() as writer:
    while True:
        row = cosmian.read_next_row(session, url, handle)
        if row is None:
            break
        writer.write_row_dict(row)
