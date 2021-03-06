# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
# import logging
from cosmian_utils import recover_datasets_info, cosmian_schema_2_dataiku_schema
from cosmian_lib import Server


dataset_names = get_input_names_for_role('datasets')
datasets = [dataiku.Dataset(name) for name in dataset_names]
url, views = recover_datasets_info(datasets)

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

# Connect to the Cosmian server
server = Server(url)
c_datasets = server.datasets()
enclave = server.enclave()

# REST request to run the protected algorithm
enclave.run_protected_algorithm(views, algo_name, output_name)

# write the output schema to the dataiku output dataset
output_dataset = dataiku.Dataset(output_name)
c_dataset = c_datasets.retrieve(output_name, False)
output_schema = c_dataset.schema()
output_dataset.write_schema(cosmian_schema_2_dataiku_schema(output_schema))

# Stream entries and write them to the output
with output_dataset.get_writer() as writer:
    while True:
        row = c_dataset.read_next_row()
        if row is None:
            break
        writer.write_row_dict(row)
