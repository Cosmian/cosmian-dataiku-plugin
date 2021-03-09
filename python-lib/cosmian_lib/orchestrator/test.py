import cosmian_lib
from cosmian_lib.orchestrator import Orchestrator
import time

ORCHESTRATOR_URL = "http://localhost:9000"


def login() -> Orchestrator:
    os = Orchestrator(ORCHESTRATOR_URL)
    os.authentication().login("hello@world.com", "azerty")
    return os


def test_login_logout():
    os = login()
    os.computations().list()
    os.authentication().logout()
    try:
        os.computations().list()
        raise ValueError("Should be logged out")
    except:
        pass


def test_computation_runs():
    os = login()
    try:
        comp_api = os.computations()
        computations = comp_api.list()
        if len(computations) == 0:
            return
        uuid = computations[0]["uuid"]
        runs = comp_api.runs(uuid).list()
        last_idx = len(runs) - 1
        assert "uuid" in runs[last_idx]
        assert "results" in runs[last_idx]
        latest = comp_api.runs(uuid).latest()
        assert runs[last_idx]["uuid"] == latest["uuid"]
        assert runs[last_idx]["results"] == latest["results"]
    finally:
        os.authentication().logout()


def test_re_run():
    os = login()
    try:
        comp_api = os.computations()
        computations = comp_api.list()
        last_index = len(computations) - 1
        print("last index", last_index)
        if last_index == -1:
            return
        computation = computations[last_index]
        uuid = computation["uuid"]
        revision = computation["revision"]
        comp_api.runs(uuid).launch(revision)
        status = ""
        while status != "finished" and status != "error":
            latest = comp_api.runs(uuid).latest()
            status = latest["status"]
            print(status)
            time.sleep(1)
    finally:
        os.authentication().logout()


if __name__ == '__main__':
    # test_login_logout()
    # test_computation_runs()
    test_re_run()
    print("SUCCESS !")
