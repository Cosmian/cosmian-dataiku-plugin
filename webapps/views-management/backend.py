from cosmian_lib import Server, Views
from dataiku.customwebapp import get_webapp_config
import requests
from flask import request, jsonify
import sys
import logging
import json

logging.info("Python version: %s" % sys.version)

server_url = get_webapp_config()['server_url']
logging.info("Cosmian server URL: %s" % server_url)
server = Server(server_url)
c_views = server.views()


@app.route('/view', methods=['POST'])
def create_or_update_view():
    override = request.headers.get_all("X-HTTP-Method-Override")
    method = "POST" if len(override) == 0 else override[0]
    if method == "POST":
        return create_view(request.json)
    return update_view(request.json)


def create_view(view):
    try:
        print("###########################")
        print(type(view))
        print(view)
        json.loads(view.decode('utf-8'))
        return jsonify(c_views.create(view))
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


def update_view(view):
    try:
        return jsonify(c_views.update(view))
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


@app.route('/views', methods=['GET'])
def list_views():
    try:
        return jsonify(c_views.all())
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


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
    try:
        return jsonify(c_views.delete(view_name))
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


def retrieve_view(view_name):
    try:
        return jsonify(c_views.retrieve(view_name))
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# Temporary until Cosmian server serves schemas

SAMPLE_VIEW = """{
    "name": "${NAME}",
    "data_source": {
        "url": "file://${ROOT_DIR}/data/${NAME}.csv",
        "meta_data": {
            "decimal_separator": 46,
            "delimiter": 44,
            "headers_on_first_line": false
        },
        "sorted": false
    },
    "schema": {
        "columns": [
            {
                "name": "id",
                "data_type": "string",
                "export": {
                    "function": "as_is"
                }
            },
            {
                "name": "fe_cmp_hash",
                "data_type": "fe_cmp_hash_sorted",
                "export": {
                    "function": "as_is"
                }
            },
            {
                "name": "float",
                "data_type": "float",
                "export": {
                    "function": "as_is"
                }
            },
            {
                "name": "int32",
                "data_type": "int32",
                "export": {
                    "function": "as_is"
                }
            },
            {
                "name": "int64",
                "data_type": "int64",
                "export": {
                    "function": "as_is"
                }
            },
            {
                "name": "ip_v4",
                "data_type": "ipv4",
                "export": {
                    "function": "as_is"
                }
            }
        ],
        "key": [
            "id"
        ]
    },
    "sort": {
        "sort_mode": "natural"
    },
    "write_mode": "truncate"
}"""

# c_views = {}
# for name in ['view-1', 'view-2', 'view-3', 'view-4', 'view-5', 'view-6']:
#     c_views[name] = SAMPLE_VIEW.replace("${NAME}", name)
