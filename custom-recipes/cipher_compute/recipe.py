# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
# import logging
from cosmian_utils import cosmian_schema_2_dataiku_schema
from cosmian_lib.orchestrator import Orchestrator

# the output dataset name
output_name = get_output_names_for_role('output')[0]

# recover the parameters
recipe_config = get_recipe_config()

if 'orchestrator_url' in recipe_config:
    orchestrator_url = recipe_config['orchestrator_url']
else:
    orchestrator_url = ''
if orchestrator_url == '':
    raise ValueError("Please provide an orchestrator URL")

if 'orchestrator_username' in recipe_config:
    orchestrator_username = recipe_config['orchestrator_username']
else:
    orchestrator_username = ''
if orchestrator_username == '':
    raise ValueError("Please provide an orchestrator username")

if 'orchestrator_password' in recipe_config:
    orchestrator_password = recipe_config['orchestrator_password']
else:
    orchestrator_password = ''
if orchestrator_password == '':
    raise ValueError("Please provide an orchestrator user passsword")

if 'computation_uuid' in recipe_config:
    computation_uuid = recipe_config['computation_uuid']
else:
    computation_uuid = ''
if computation_uuid == '':
    raise ValueError("Please provide a computation UUID")


# recover results
print("*****",orchestrator_username,orchestrator_password)
os = Orchestrator(orchestrator_url)
os.authentication().login(orchestrator_username, orchestrator_password)
try:
    comp_api = os.computations()
    computation = comp_api.retrieve(computation_uuid)
    latest = comp_api.runs(computation_uuid).latest()
    results = latest["results"]
    print(results)
finally:
    os.authentication().logout()

print("***\n***\n***\nResults",results)    
    
if len(results) == 0:
    raise ValueError("No results")

di_schema = []
for col in range(len(results[0])):
    di_schema.append(
        {"name": f"c{col}", "type": "int"})

# write the output schema to the dataiku output dataset
output_dataset = dataiku.Dataset(output_name)
output_dataset.write_schema(di_schema)

# Stream entries and write them to the output
with output_dataset.get_writer() as writer:
    for row in range(len(results)):
        writer.write_row_array(results[row])
