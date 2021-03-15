from typing import List, Tuple
from cosmian_lib import Context
import time


def retrieve_computation(context: Context, uuid: str) -> dict:
    return context.get(f"/computations/{uuid}", None,
                       f"Computations:: failed retrieving computation: {uuid}")


class Run():
    def __init__(self, run_dict: dict):
        self.__dict__ = run_dict


class Computation():

    def __init__(self, context: Context, computation_dict: dict):
        self.__dict__ = computation_dict
        self.context = context

    def delete(self) -> dict:
        """
        Delete this computation
        """
        return self.context.delete(f"/computations/{self.uuid}", None,
                                   f"Computations:: failed deleting the computation: {self.uuid}")

    def update(self) -> dict:
        """
        Update this computation
        """
        dic = vars(self)
        del dic["context"]
        return self.context.put(f"/computations/{self.uuid}", dic,
                                f"Computations:: failed updating the computation: {self.uuid}")

    def duplicate(self):
        """
        Duplicate the computation with the given UUID
        """
        return Computation(self.context,
                           self.context.post(
                               f"/computations/{self.uuid}/duplicate",
                               None,
                               f"Computations:: failed duplicating the computation: {self.uuid}"
                           ))

    def list_runs(self) -> List[str]:
        """
        List all the computation runs UUIds
        """
        rs = self.context.get(f"/computations/{self.uuid}/runs", None,
                              f"Computation Runs:: failed listing the run UUIDs for computation: {self.uuid}")
        return [r["uuid"] for r in rs]

    def latest_run(self) -> Run:
        """
        Retrieve the latest run of the computation
        """
        return Run(self.context.get(
            f"/computations/{self.uuid}/runs/latest",
            None,
            f"Computation Runs:: failed the latest run for computation: {self.uuid}"
        ))

    def retrieve_run(self, run_uuid: str) -> Run:
        """
        Retrieve the given run of the computation
        """
        return Run(self.context.get(
            f"/computations/{self.uuid}/runs/{run_uuid}",
            None,
            f"Computation Runs:: failed: the run {run_uuid} for computation: {self.uuid}"
        ))

    def launch(self, revision_id="") -> dict:
        """
        (Re) Launches a computation for the given revision id or last revision if no supplied.
        If that ID is not known, the computation is read from the server first and the last revision is used.
        Returns a fresh copy of the computation
        """
        if revision_id == "":
            revision_id = self.dict["revision"]
        return self.context.post(f"/computations/{self.uuid}/queue", {"revision": revision_id},
                                 f"Computation:: failed launching computation: {self.uuid}")

    def wait_for_completion(self) -> Tuple[str, str]:
        """
        Wait for the last run to complete.
        Returns the rum uuid and the status
        """
        # Start by waiting for one second as the last run may not
        # be immediately available
        status = ""
        while status != "finished" and not status.startswith("error"):
            time.sleep(1)
            latest = self.latest_run()
            status = latest["status"]
            uuid = latest["uuid"]
        return (uuid, status)


class ComputationsAPI():

    def __init__(self, context: Context):
        self.context = context

    def list(self) -> List[str]:
        """
        List all the computations UUIDs
        """
        cs = self.context.get("/computations", None,
                              "Computations::failed listing the computations")
        return [c["uuid"] for c in cs]

    def retrieve(self, uuid: str) -> Computation:
        """
        Retrieve a given computation using its UUID
        """
        computation_dict = retrieve_computation(self.context, uuid)
        return Computation(self.context, computation_dict)
