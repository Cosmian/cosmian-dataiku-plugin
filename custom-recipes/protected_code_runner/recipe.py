# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *
import logging
import cosmian
import requests


# an HTTP 1.1 session with keep-alive
session = requests.Session()

dataset_names = get_input_names_for_role('datasets')
datasets = [dataiku.Dataset(name) for name in dataset_names]
url, views = cosmian.recover_datasets_info(datasets)

# Join parameters
recipe_config = get_recipe_config()
algo_name = recipe_config['algo_name']
# For MCFE joins, retrieve the inner join key
if 'algo_name' in recipe_config:
    algo_name = recipe_config['algo_name']
else:
    algo_name = ''
if algo_name == '':
    raise ValueError("Please provide an algorithm name")

# REST request to run the protected algorith
handle = cosmian.run_protected_algorithm(
    session, url, views,
    algo_name)

output_dataset = dataiku.Dataset(get_output_names_for_role('output')[0])
output_schema = cosmian.get_schema(session, url, handle)
output_dataset.write_schema(output_schema)

# Stream entries and write them to the output
with output_dataset.get_writer() as writer:
    while True:
        row = cosmian.read_next_row(session, url, handle)
        if row is None:
            break
        writer.write_row_dict(row)



# # -*- coding: utf-8 -*-
# import dataiku
# import pandas as pd, numpy as np
# from dataiku import pandasutils as pdu
#
# # Read recipe inputs
# ages = dataiku.Dataset("ages")
# ages_df = ages.get_dataframe()
# salaries = dataiku.Dataset("salaries")
# salaries_df = salaries.get_dataframe()
#
#
# # Compute recipe outputs
# # TODO: Write here your actual code that computes the outputs
# # NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.
#
# test2_df = ... # Compute a Pandas dataframe to write into test2
#
#
# # Write recipe outputs
# test2 = dataiku.Dataset("test2")
# test2.write_with_schema(test2_df)