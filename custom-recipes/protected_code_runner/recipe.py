# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *
# import logging
import cosmian
import requests
import time

# an HTTP 1.1 session with keep-alive
session = requests.Session()

dataset_names = get_input_names_for_role('datasets')
datasets = [dataiku.Dataset(name) for name in dataset_names]
url, views = cosmian.recover_datasets_info(datasets)

output_name = get_output_names_for_role('output')[0]

# Join parameters
recipe_config = get_recipe_config()
# recover the mandatory algo name
if 'algo_name' in recipe_config:
    algo_name = recipe_config['algo_name']
else:
    algo_name = ''
if algo_name == '':
    raise ValueError("Please provide an algorithm name")

# REST request to run the protected algorith
cosmian.run_protected_algorithm(
    session, url, views,
    algo_name, output_name)

output_dataset = dataiku.Dataset(output_name+"-"+time.time())
handle = cosmian.get_dataset_handle(session, url, output_name, False)
output_schema = cosmian.get_schema(session, url, handle)
output_dataset.write_schema(output_schema)

# Stream entries and write them to the output
output_dataset.get_writer().
with output_dataset.get_writer() as writer:
    while True:
        row = cosmian.read_next_row(session, url, handle)
        if row is None:
            break
        writer.write_row_dict(row)
