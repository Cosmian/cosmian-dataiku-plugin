from flask import request, jsonify
import requests
from dataiku.customwebapp import get_webapp_config
from cosmian_lib import Server


server_url = get_webapp_config()['server_url']
# if not server_url.endswith("/"):
#     server_url += "/"
server = Server(server_url)
views = server.views()


@app.route('/view', methods=['POST'])
def create_or_update_view():
    override = request.headers.get_all("X-HTTP-Method-Override")
    method = "POST" if len(override) == 0 else override[0]
    if method == "POST":
        return create_view(request.data)
    return update_view(request.data)


def create_view(view):
    try:
        return jsonify(views.create(view))
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


def update_view(view):
    try:
        return jsonify(views.update(view))
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


@app.route('/views', methods=['GET'])
def list_views():
    try:
        return jsonify(views.list())
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
        return jsonify(views.delete(view_name))
    except ValueError as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


def retrieve_view(view_name):
    try:
        return jsonify(views.retrieve(view_name))
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

views = {}
for name in ['view-1', 'view-2', 'view-3', 'view-4', 'view-5', 'view-6']:
    views[name] = SAMPLE_VIEW.replace("${NAME}", name)
