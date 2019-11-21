from cosmian import cosmian
from flask import request
import json
import requests

# Example:
# From JavaScript, you can access the defined endpoints using
# getWebAppBackendUrl('first_api_call')

# an HTTP 1.1 session with keep-alive
session = requests.Session()


@app.route('/deploy_code', methods=['POST'])
def deploy_code():
    print("************************ YEAH")
    data = request.json
    # check inputs
    if 'local_server_url' in data:
        local_server_url = data['local_server_url']
    else:
        local_server_url = ''
    if local_server_url.strip() == '':
        return json.dumps({'status': 'error', 'msg': 'Please provide your Cosmian server URL'})
    if not local_server_url.endswith("/"):
        local_server_url += "/"

    if 'remote_server_url' in data:
        remote_server_url = data['remote_server_url']
    else:
        remote_server_url = ''
    if remote_server_url.strip() == '':
        return json.dumps({'status': 'error', 'msg': 'Please provide the remote server url'})

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

    try:
        cosmian.deploy_python_code(session, local_server_url, remote_server_url, algo_name, python_code)
        return json.dumps({'status': 'ok', 'msg': 'Deployed "' + algo_name + '" to ' + remote_server_url})
    except ValueError as e:
        return json.dumps({'status': 'error', 'msg': str(e)}), 500

#    mydataset = dataiku.Dataset("REPLACE_WITH_YOUR_DATASET_NAME")
#    mydataset_df = mydataset.get_dataframe(sampling='head', limit=500)

#    #Pandas dataFrames are not directly JSON serializable, use to_json()
#    data = mydataset_df.to_json()
#    return json.dumps({"status": "ok", "data": data})
