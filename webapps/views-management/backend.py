import dataiku
import pandas as pd
import logging
import cosmian
from flask import request, jsonify
import json
import requests

# Example:
# From JavaScript, you can access the defined endpoints using
# getWebAppBackendUrl('first_api_call')

# an HTTP 1.1 session with keep-alive
session = requests.Session()


@app.route('/view', methods=['POST'])
def create_or_update_view():
    override = request.headers.get_all("X-HTTP-Method-Override")
    method = "POST" if len(override) == 0 else override[0]
    if method == "POST":
        return create_view(request.json)
    return update_view(request.json)


def create_view(view):
    if view.name in views:
        return jsonify({'status': 'error', 'msg': 'create view falied: ' + view.name + ', already exists'}), 400
    try:
        # cosmian.deploy_python_code(session, local_server_url, remote_server_url, algo_name, python_code)
        views[view.name] = view
        return jsonify({'status': 'ok', 'msg': 'Added: ' + view.name})
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


def update_view(view):
    if view.name not in views:
        return jsonify({'status': 'error', 'msg': 'update view failed: ' + view.name + ', does not exist'}), 400
    try:
        # cosmian.deploy_python_code(session, local_server_url, remote_server_url, algo_name, python_code)
        views[view.name] = view
        return jsonify({'status': 'ok', 'msg': 'Updated: ' + view.name})
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


@app.route('/views', methods=['GET'])
def list_views():
    print("LIST VIEWS %s" % jsonify(list(views.keys())))
    return jsonify(list(views.keys()))


@app.route('/views/json_schema', methods=['GET'])
def json_schema():
    # ignored for now
    return jsonify({})


@app.route('/view/<string:view_name>', methods=['GET'])
def retrieve_or_delete_view(view_name):
    override = request.headers.get_all("X-HTTP-Method-Override")
    method = "GET" if len(override) == 0 else override[0]
    if method == "DELETE":
        return delete_view(view_name)
    return retrieve_view(view_name)


def delete_view(view_name):
    if view_name not in views:
        return jsonify({'status': 'error', 'msg': 'delete view failed: ' + view_name + ', does not exist'}), 404
    try:
        # cosmian.deploy_python_code(session, local_server_url, remote_server_url, algo_name, python_code)
        del views[view_name]
        return jsonify({'status': 'ok', 'msg': 'Deleted: ' + view_name})
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


def retrieve_view(view_name):
    if view_name not in views:
        return jsonify({'status': 'error', 'msg': 'get view failed: ' + view_name + ', does not exist'}), 404
    try:
        # cosmian.deploy_python_code(session, local_server_url, remote_server_url, algo_name, python_code)
        return jsonify(views[view_name])
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


SAMPLE_VIEW = """{
    'name'        '${NAME}',
    'data_source': {
        'url'       'file://${ROOT_DIR}/data/sample_data.csv',
        'meta_data': {
            'decimal_separator':     46, // .
            'delimiter':             44, // ,
            'headers_on_first_line': false
        },
        'sorted':    false
    },
    'schema':      {
        'columns': [
            {
                'name'      'id',
                'data_type': PrivateDataType.String,
                'export':    {
                    'function' 'as_is',
                },
            },
            {
                'name'      'fe_cmp_hash',
                'data_type': PrivateDataType.FeCmpHashSorted,
                'export':    {
                    'function' 'as_is',
                },
            },
            {
                'name'      'float',
                'data_type': PrivateDataType.Float,
                'export':    {
                    'function' 'as_is',
                },
            },
            {
                'name'      'int32',
                'data_type': PrivateDataType.Int32,
                'export':    {
                    'function' 'as_is',
                },
            },
            {
                'name'      'int64',
                'data_type': PrivateDataType.Int64,
                'export':    {
                    'function' 'as_is',
                },
            },
            {
                'name'      'ip_v4',
                'data_type': PrivateDataType.IPv4,
                'export':    {
                    'function' 'as_is',
                },
            }
        ],
        'key':     [
            'id'
        ]
    },
    'sort':        {
        'sort_mode': SortMode.Natural,
    },
    'write_mode':  WriteMode.Truncate
}"""

views = {}
for name in ['view-1', 'view-2', 'view-3', 'view-4', 'view-5', 'view-6']:
    views[name] = SAMPLE_VIEW.replace("${NAME}", name)
