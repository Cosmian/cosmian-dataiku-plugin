import dataiku
import pandas as pd
import logging
from flask import request
import json


# Example:
# From JavaScript, you can access the defined endpoints using
# getWebAppBackendUrl('first_api_call')

@app.route('/deploy_code', methods=['POST'])
def deploy_code():
    print("************************ YEAH")
    data = request.json
    # check inputs
    if 'hostname' in data:
        hostname = data['hostname']
    else:
        hostname = ''
    if hostname.strip() == '':
        return json.dumps({'status': 'error', 'msg': 'Please provide the remote host name'})
    if 'algo_name' in data:
        algo_name = data['algo_name']
    else:
        algo_name = ''
    if algo_name.strip() == '':
        return json.dumps({'status': 'error', 'msg': 'Please provide the algorithm name'})
    if 'python_code' in data:
        python_code = data['python_code']
    else:
        python_code = ''
    if python_code.strip() == '':
        return json.dumps({'status': 'error', 'msg': 'Please provide python code'})

    # recover the server URL from the parameters
    # server_url = get_webapp_config()['server_url']

    #    code = json['python_code']
    #    code=request.data
    return json.dumps({"status": "ok", "msg": data['server_url']})
#    mydataset = dataiku.Dataset("REPLACE_WITH_YOUR_DATASET_NAME")
#    mydataset_df = mydataset.get_dataframe(sampling='head', limit=500)

#    #Pandas dataFrames are not directly JSON serializable, use to_json()
#    data = mydataset_df.to_json()
#    return json.dumps({"status": "ok", "data": data})
