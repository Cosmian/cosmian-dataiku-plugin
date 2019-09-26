# IP MPC

import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *
import logging
import cosmian
import requests

logging.warn("****Starting IP MPC")

# an HTTP 1.1 session with keep-alive
session = requests.Session()

# recover inoput datasets
dataset_names = get_input_names_for_role('datasets')
datasets = [dataiku.Dataset(name) for name in dataset_names]

datasets_info = cosmian.recover_datasets_info(datasets)

# REST request to inner join
handle = cosmian.run_ip_mpc(session, datasets_info.url, datasets_info.views)

output_dataset = dataiku.Dataset(get_output_names_for_role('output')[0])
output_schema = cosmian.get_schema(session, datasets_info.url, handle)
output_dataset.write_schema(output_schema)

logging.warn("****Done executing, writing output dataset")

# Stream entries and write them to the output
with output_dataset.get_writer() as writer:
    while True:
        row = cosmian.read_next_row(session, datasets_info.url, handle)
        if row is None:
            break
        writer.write_row_dict(row)