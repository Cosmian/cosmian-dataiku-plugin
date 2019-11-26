import requests
import json


def list_views(session, server_url):
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
        "Content-type": "application/json",
        "x-http-method-override": "GET"
    }
    try:
        r = session.get(
            url="%sviews" % server_url,
            headers=headers
        )
        if r.status_code != 200:
            raise ValueError("Cosmian Server:: Error listing views, status code: %s, reason :%s" % (
                r.status_code, r.text))
        return r.json()
    except requests.ConnectionError as e:
        raise ValueError("List Views: failed querying Cosmian Server at: %s, error: %s" % (server_url, e))


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
            raise ValueError("Cosmian Server:: Error deleting view: %s, status code: %s, reason :%s" % (
                view_name, r.status_code, r.text))
    except requests.ConnectionError as e:
        raise ValueError("Delete View: failed querying Cosmian Server at: %s, error: %s" % (server_url, e))


def retrieve_view(session, server_url, view_name):
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
        "Content-type": "application/json",
        "x-http-method-override": "GET"
    }
    try:
        r = session.get(
            url="%sview/%s" % (server_url, view_name),
            headers=headers
        )
        if r.status_code != 200:
            raise ValueError("Cosmian Server:: Error retrieving view: %s, status code: %s, reason :%s" % (
                view_name, r.status_code, r.text))
        return r.json()
    except requests.ConnectionError as e:
        raise ValueError("Retrieve View: failed querying Cosmian Server at: %s, error: %s" % (server_url, e))


def update_view(session, server_url, view):
    headers = {
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
        "Content-type": "application/json",
        "x-http-method-override": "PUT"
    }
    data = json.loads(view)
    try:
        r = session.post(
            url="%sview" % server_url,
            headers=headers,
            json=data,
        )
        if r.status_code != 200:
            raise ValueError("Cosmian Server:: Error updating view, status code: %s, reason :%s" % (
                r.status_code, r.text))
        return {'status': 'ok', 'msg': 'Updated: ' + data.name}
    except requests.ConnectionError as e:
        print("****ERROR is there ", str(e))
        raise ValueError("Updating View: failed querying Cosmian Server at: %s, error: %s" % (server_url, e))
