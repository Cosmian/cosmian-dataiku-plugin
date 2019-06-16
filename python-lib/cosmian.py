# DSS automatically add this file as a module
# In your IDE it should be sym-linked to libs ot the python Env.

import requests


def get_dataset_handle(session, server_url, view, sorted):
    # attempt to create open a source to this dataset
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    params = {}
    try:
        dataset_sort_path = "sorted_dataset" if sorted else "raw_dataset"
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


def get_inner_join_handle(session, server_url, left_view, right_view):
    # attempt to create open a source to this dataset
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    params = {}
    try:
        r = session.get(
            url="%sinner_join/%s/%s" % (server_url, left_view, right_view),
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
    except requests.ConnectionError:
        raise ValueError("Failed querying Cosmian Server at: %s" % server_url)


def cosmian_type_2_dataiku_type(ct):
    if ct == "hash":
        return "string"
    if ct == "int32":
        return "int"
    if ct == "int64":
        return "bigint"
    if ct == "float":
        return "double"
    if ct == "string" or ct == "hash" or ct == "blurred" or ct == "encrypted":
        return "string"
    return "object"
