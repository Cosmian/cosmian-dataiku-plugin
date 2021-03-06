import cosmian_lib
from cosmian_lib.orchestrator import Orchestrator


ORCHESTRATOR_URL = "http://localhost:9000"


def test_login_logout():
    os = Orchestrator(ORCHESTRATOR_URL)
    os.authentication().login("hello@world.com", "azerty")
    os.computations().list()
    os.authentication().logout()
    try:
        os.computations().list()
        raise ValueError("Should be logged out")
    except:
        pass


# def test_computation_run():
#     os = Orchestrator(ORCHESTRATOR_URL)
#     os.authentication().login("hello@world.com", "azerty")
#     try:
#         comp_api = os.computations()
#         computations = comp_api.list()
#         if len(computations) == 0:
#             return
#         uuid = computations[0]["uuid"]
#         run = comp_api.run(uuid)
#         assert "uuid" in run
#         assert "results" in run
#     finally:
#         os.authentication().logout()


def test_computation_runs():
    os = Orchestrator(ORCHESTRATOR_URL)
    os.authentication().login("hello@world.com", "azerty")
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


if __name__ == '__main__':
    test_login_logout()
    test_computation_runs()
    print("SUCCESS !")
