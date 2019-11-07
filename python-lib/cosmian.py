# DSS automatically add this file as a module
# In your IDE it should be sym-linked to libs ot the python Env.

import requests
import json
from urlparse import urlparse


def get_dataset_handle(session, server_url, view, sort_ds):
    # attempt to create open a source to this dataset
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    params = {}
    try:
        dataset_sort_path = "sorted_dataset" if sort_ds else "raw_dataset"
        r = session.get(
            url="%sview/%s/%s" % (server_url, view, dataset_sort_path),
            params=params,
            headers=headers
        )
        if r.status_code != 200:
            raise ValueError("Cosmian Server:: Error querying view: %s, status code: %s, reason :%s" % (
                view, r.status_code, r.text))
        resp = r.json()
        return resp["handle"]
    except requests.ConnectionError:
        raise ValueError("Failed establishing connection to the Cosmian Server at: %s" % server_url)


def get_join_v1_handle(session, server_url, left_view, right_view, join_type, outer_join_index, join_key):
    # attempt to create open a source to this dataset
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    params = {
        "join_type": join_type,
        "outer_join_index": outer_join_index,
        "join_key": join_key
    }
    try:
        r = session.get(
            url="%sjoin/%s/%s" % (server_url, left_view, right_view),
            params=params,
            headers=headers
        )
        if r.status_code != 200:
            raise ValueError("Cosmian Server:: Error querying inner join: %s/%s, status code: %s, reason :%s" % (
                left_view, right_view, r.status_code, r.text))
        resp = r.json()
        return resp["handle"]
    except requests.ConnectionError:
        raise ValueError("Failed establishing connection to the Cosmian Server at: %s" % server_url)


def get_join_handle(session, server_url, views, join_type, compute_key):
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    params = {
        'views': json.dumps(views),
        "join_type": join_type,
        "compute_key": compute_key
    }
    try:
        r = session.get(
            url="%smerge_join" % server_url,
            params=params,
            headers=headers
        )
        if r.status_code != 200:
            raise ValueError("Cosmian Server:: Error querying join on  %s, status code: %s, reason :%s" % (
                views, r.status_code, r.text))
        resp = r.json()
        return resp["handle"]
    except requests.ConnectionError:
        raise ValueError("Failed establishing connection to the Cosmian Server at: %s" % server_url)


# noinspection DuplicatedCode
def run_ip_mpc(session, server_url, views):
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    params = {
        'views': json.dumps(views),
        "join_type": "recurring",  # FIXME this will have to go after the OVH demo
    }
    try:
        r = session.get(
            url="%smerge_join" % server_url,
            params=params,
            headers=headers
        )
        if r.status_code != 200:
            raise ValueError("Cosmian Server:: Error run mpc on %s, status code: %s, reason :%s" % (
                views, r.status_code, r.text))
        resp = r.json()
        return resp["handle"]
    except requests.ConnectionError as e:
        raise ValueError("IP Identification: failed querying Cosmian Server at: %s, error: %s" % (server_url, e))


def run_linear_regression(session, server_url, views, s_mode, columns, range_start, range_end):
    if s_mode == 'stack':
        mode = 'aggregate_datasets'
    else:
        mode = 'split_dimensions'
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    params = {
        'views': json.dumps(views),
        'columns': json.dumps(columns),
        'mode': mode,
        'range_start': range_start,
        'range_end': range_end,
    }
    print("Linear regression params: ", params)
    try:
        r = session.get(
            url="%slinear_regression" % server_url,
            params=params,
            headers=headers
        )
        if r.status_code != 200:
            raise ValueError(
                "Cosmian Server:: Error running MPC linear regression on %s, status code: %s, reason :%s" % (
                    views, r.status_code, r.text))
        resp = r.json()
        return resp["handle"]
    except requests.ConnectionError as e:
        raise ValueError("Linear Regression: failed querying Cosmian Server at: %s, error: %s" % (server_url, e))


def get_schema(session, server_url, handle):
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    params = {}
    try:
        r = session.get(
            url="%sdataset/%s/schema" % (server_url, handle),
            params=params,
            headers=headers
        )
        if r.status_code != 200:
            raise ValueError("Cosmian Server:: Error querying dataset: %s, status code: %s, reason :%s" % (
                handle, r.status_code, r.text))
        schema = r.json()
        response = []
        for col in schema["columns"]:
            response.append({"name": col["name"], "type": cosmian_type_2_dataiku_type(col["data_type"])})
        return response
    except requests.ConnectionError:
        raise ValueError("Failed connecting to Cosmian Server at: %s" % server_url)


def read_next_row(session, server_url, handle):
    try:
        headers = {
            "Accept-Encoding": "gzip",
            "Accept": "application/json"
        }
        params = {}
        r = session.get(
            url="%sdataset/%s/next" % (server_url, handle),
            params=params,
            headers=headers
        )
        if r.status_code == 404:  # EOF
            return None
        if r.status_code == 200:
            return r.json()
        else:
            raise ValueError("Cosmian Server:: Error querying dataset: %s, status code: %s, reason :%s" % (
                handle, r.status_code, r.text))
    except requests.ConnectionError as e:
        raise ValueError("Read next row: failed querying Cosmian Server at: %s, error: %s" % (server_url, e))


def deploy_python_code(session, local_server_url, remote_server_url, algo_name, python_code):
    # FIXME: later on we want to pass the remote server url directly without parsing
    parsed_remote = urlparse(remote_server_url)
    if parsed_remote.netloc == '':
        return json.dumps({'status': 'error', 'msg': 'Invalid remote server url'})
    remote_hostname = parsed_remote.netloc

    try:
        headers = {
            "Accept-Encoding": "gzip",
            "Accept": "application/json",
            "Content-type": "application/json"
        }
        data = {
            'hostname': remote_hostname,
            'algo_name': algo_name,
            'code': python_code
        }
        r = session.post(
            url="%senclave/code_update" % local_server_url,
            json=data,
            headers=headers
        )
        if r.status_code == 200:
            return
        else:
            raise ValueError("Cosmian Server:: Error deploying protected code to: %s, status code: %s, reason :%s" % (
                remote_server_url, r.status_code, r.text))
    except requests.ConnectionError:
        raise ValueError("Failed querying Cosmian Server at: %s" % local_server_url)


def delete_view(session, server_url, view_name):
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
        "Content-type": "application/json",
        "x-http-method-override": "DELETE"
    }
    try:
        r = session.get(
            url="%sview/%s" % (server_url, view_name),
            headers=headers
        )
        if r.status_code != 200 and r.status_code != 404:
            raise ValueError("Cosmian Server:: Error Deleting View: %s, status code: %s, reason :%s" % (
                view_name, r.status_code, r.text))
    except requests.ConnectionError as e:
        raise ValueError("Read next row: failed querying Cosmian Server at: %s, error: %s" % (server_url, e))


def run_protected_algorithm(session, server_url, views, algo_name, output_name):
    # first delete an existing output view
    delete_view(session,server_url,output_name)
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
        "Content-type": "application/json"
    }
    data = {
        'server_url': server_url,
        'views': views,
        'output_name': output_name
    }
    params = json.dumps(data)
    try:
        r = session.post(
            url="%senclave/run_code/%s" % (server_url, algo_name),
            json={'params': params},
            headers=headers
        )
        if r.status_code != 200:
            raise ValueError("Cosmian Server:: Error running algorithm: %s, status code: %s, reason :%s" % (
                algo_name, r.status_code, r.text))
        resp = r.json()
        print("Run Code Response: ", resp)
    except requests.ConnectionError as e:
        raise ValueError("Read next row: failed querying Cosmian Server at: %s, error: %s" % (server_url, e))


def cosmian_type_2_dataiku_type(ct):
    if ct == "hash":
        return "string"
    if ct == "int32":
        return "int"
    if ct == "int64":
        return "bigint"
    if ct == "float":
        return "double"
    if ct == "string" or ct == "hash" or ct == "blurred" or ct == "encrypted" or ct == "fe_cmp_encrypted":
        return "string"
    return "object"


def recover_datasets_info(datasets):
    url = ''
    views = []
    # process the list of datasets
    # collect the views which will be passed as parameter to the call
    # views must all be from the same server
    for dataset in datasets:
        config = dataset.get_config()['params']['customConfig']
        view = config['view_name']
        # sorted = config['sorted']
        server_url = config['server_url']
        if not server_url.endswith("/"):
            server_url += "/"
        if url == '':
            url = server_url
        else:
            if url != server_url:
                raise ValueError("All datasets must be accessed from the same Cosmian server")
        views.append(view)
    return url, views
