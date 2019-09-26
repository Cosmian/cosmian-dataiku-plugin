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
import cosmian
import requests

logging.warn("****RECIPE RESOURCE: %s", get_recipe_resource())

# an HTTP 1.1 session with keep-alive
session = requests.Session()

dataset_names = get_input_names_for_role('datasets')
datasets = [dataiku.Dataset(name) for name in dataset_names]

url = ''
views = []

# process the list of datasets
# collect the views which will be passed as parameter to the call
# views must all be from the same server
for dataset in datasets:
    config = dataset.get_config()['params']['customConfig']
    logging.warn("******** Cosmian: Dataset Config: %s", config)
    view = config['view_name']
    # sorted = config['sorted']
    server_url = config['server_url']
    if not server_url.endswith("/"):
        server_url += "/"
    if url == '':
        url = server_url
    else:
        if url != server_url:
            raise ValueError("All datasets must be accessed from the same Cosmian server")
    views.append(view)

# Join parameters
recipe_config = get_recipe_config()
join_type = recipe_config['join_type']
# For MCFE joins, retrieve the inner join key
if 'join_key' in recipe_config:
    join_key = recipe_config['join_key']
else:
    join_key = ''

# REST request to inner join
handle = cosmian.get_join_handle(
    session, url, views,
    join_type, join_key)

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
