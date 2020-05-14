# IP MPC

import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *
import logging
from cosmian_utils import recover_datasets_info, cosmian_schema_2_dataiku_schema
from cosmian_lib import Server

logging.warn("****Starting IP MPC")

# Join parameters
recipe_config = get_recipe_config()
computation = recipe_config['computation']

# recover input datasets
dataset_names = get_input_names_for_role('datasets')
datasets = [dataiku.Dataset(name) for name in dataset_names]
url, views = recover_datasets_info(datasets)

# connect to a Cosmian server
server = Server(url)
mpc = server.mpc()

if computation == 'ip_identification':
    c_dataset = mpc.run_ip_mpc(views)

elif computation == 'linear_regression_stack':
    columns = [recipe_config['column_1'], recipe_config['column_2']]
    range_start = recipe_config['range_start']
    range_end = recipe_config['range_end']
    c_dataset = mpc.run_linear_regression(
        views, 'stack', columns, range_start, range_end)

elif computation == 'linear_regression_join':
    columns = [recipe_config['column_1'], recipe_config['column_2']]
    range_start = recipe_config['range_start']
    range_end = recipe_config['range_end']
    c_dataset = mpc.run_linear_regression(
        views, 'join', columns, range_start, range_end)

else:
    raise ValueError("unknown algorithm: {%s}" % computation)

output_dataset = dataiku.Dataset(get_output_names_for_role('output')[0])
output_schema = c_dataset.get_schema()
output_dataset.write_schema(cosmian_schema_2_dataiku_schema(output_schema))

logging.warn("****Done executing, writing output dataset")

# Stream entries and write them to the output
with output_dataset.get_writer() as writer:
    while True:
        row = c_dataset.read_next_row()
        if row is None:
            break
        writer.write_row_dict(row)
