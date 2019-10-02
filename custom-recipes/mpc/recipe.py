# IP MPC

import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *
import logging
import cosmian
import requests

logging.warn("****Starting IP MPC")

# Join parameters
recipe_config = get_recipe_config()
computation = recipe_config['computation']

# recover input datasets
dataset_names = get_input_names_for_role('datasets')
datasets = [dataiku.Dataset(name) for name in dataset_names]
url, views = cosmian.recover_datasets_info(datasets)

# an HTTP 1.1 session with keep-alive
session = requests.Session()
# REST request to inner join
if computation == 'ip_identification':
    handle = cosmian.run_ip_mpc(session, url, views)
elif computation == 'linear_regression_stack':
    handle = cosmian.run_linear_regression(session, url, views, 'stack')
elif computation == 'linear_regression_join':
    handle = cosmian.run_linear_regression(session, url, views, 'join')
else:
    raise ValueError("unknown algorithm: {%s}" % computation)

output_dataset = dataiku.Dataset(get_output_names_for_role('output')[0])
output_schema = cosmian.get_schema(session, url, handle)
output_dataset.write_schema(output_schema)

logging.warn("****Done executing, writing output dataset")

# Stream entries and write them to the output
with output_dataset.get_writer() as writer:
    while True:
        row = cosmian.read_next_row(session, url, handle)
        if row is None:
            break
        writer.write_row_dict(row)
