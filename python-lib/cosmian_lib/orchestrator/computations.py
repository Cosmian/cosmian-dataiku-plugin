from cosmian_lib import Context


def retrieve_computation(context: Context, uuid: str) -> dict:
    return context.get(f"/computations/{uuid}", None,
                       f"Computations:: failed retrieving computation: {uuid}")


class Runs():
    """
    A computation runs
    """

    def __init__(self, context: Context, computation_uuid: str):
        self.context = context
        self.computation_uuid = computation_uuid

    def list(self):
        """
        List all the computation runs
        """
        return self.context.get(f"/computations/{self.computation_uuid}/runs", None, f"Computation Runs:: failed listing the runs for computation: {self.computation_uuid}")

    def latest(self) -> dict:
        """
        Retrieve the latest run of the computation
        """
        return self.context.get(f"/computations/{self.computation_uuid}/runs/latest", None, f"Computation Runs:: failed the latest run for computation: {self.computation_uuid}")

    def retrieve(self, run_uuid: str) -> dict:
        """
        Retrieve the given run of the computation
        """
        return self.context.get(f"/computations/{self.computation_uuid}/runs/{run_uuid}", None,
                                f"Computation Runs:: failed the run {run_uuid} for computation: {self.computation_uuid}")

    def launch(self, revision_id="") -> dict:
        """
        (Re) Launches a computation for its last revision id.
        If that ID is not known, the computation is read from the server first and the last revision is used.
        Returns a fresh copy of the computation
        """
        if revision_id == "":
            computation = retrieve_computation(
                self.context, self.computation_uuid)
            revision_id = computation.revision
        return self.context.post(f"/computations/{self.computation_uuid}/queue", {"revision": revision_id},
                                 f"Computation:: failed launching computation: {self.computation_uuid}")


class Computations():

    def __init__(self, context: Context):
        self.context = context

    def list(self):
        """
        List all the computations
        """
        return self.context.get("/computations", None, "Computations::failed listing the computations")

    def retrieve(self, uuid: str) -> dict:
        """
        Retrieve a given computation using its UUID
        """
        retrieve_computation(self.context, uuid)

    def runs(self, computation_uuid: str) -> Runs:
        """
        Access to the computation runs Api
        """
        return Runs(self.context, computation_uuid)
