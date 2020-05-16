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
import logging
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config, get_recipe_resource
from cosmian_utils import recover_datasets_info, cosmian_schema_2_dataiku_schema
from cosmian_lib import Server

logging.info("****MERGE JOIN recipe resource: %s", get_recipe_resource())

dataset_names = get_input_names_for_role('datasets')
datasets = [dataiku.Dataset(name) for name in dataset_names]
url, views = recover_datasets_info(datasets)

logging.info("****MERGE JOIN recipe url  : %s", url)
logging.info("****MERGE JOIN recipe views: %s", views)

# Join parameters
recipe_config = get_recipe_config()
join_type = recipe_config['join_type']
# For MCFE joins, retrieve the inner join key
if 'compute_key' in recipe_config:
    compute_key = recipe_config['compute_key']
else:
    compute_key = ''

# Connect to the Cosmian server
server = Server(url)
fe = server.fe()

# REST request to inner join
c_dataset = fe.get_join_dataset(views, join_type, compute_key)

output_dataset = dataiku.Dataset(get_output_names_for_role('output')[0])
output_schema = c_dataset.schema()
output_dataset.write_schema(cosmian_schema_2_dataiku_schema(output_schema))

# Stream entries and write them to the output
with output_dataset.get_writer() as writer:
    while True:
        row = c_dataset.read_next_row()
        if row is None:
            break
        writer.write_row_dict(row)
