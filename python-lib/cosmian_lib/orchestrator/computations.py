from cosmian_lib import Context


class Runs():
    """
    A computation runs
    """

    def __init__(self, context: Context, uuid: str):
        self.context = context
        self.uuid = uuid

    def list(self):
        """
        List all the computation runs
        """
        return self.context.get(f"/computations/{self.uuid}/runs", None, f"Computation Runs:: failed listing the runs for computation: {self.uuid}")

    def latest(self) -> dict:
        """
        Retrieve the latest run of the computation
        """
        return self.context.get(f"/computations/{self.uuid}/runs/latest", None, f"Computation Runs:: failed the latest run for computation: {self.uuid}")


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
        return self.context.get(f"/computations/{uuid}", None, f"Computation Runs:: failed retrieving computation: {uuid}")

    # def run(self, uuid: str) -> dict:
    #     """
    #     DEPRECATED
    #     Returns the last run of the computation
    #     """
    #     return self.context.get(f"/computations/{uuid}/run", None, f"Computation Run:: failed retrieving the last run for computation {uuid}")

    def runs(self, uuid: str) -> Runs:
        """
        Access to the computation runs Api        
        """
        return Runs(self.context, uuid)
