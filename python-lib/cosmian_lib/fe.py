import json
from .context import Context
from .dataset import Dataset


class FE():

    def __init__(self, context: Context):
        self.context = context

    def get_join_dataset(self, views, join_type, compute_key):
        """
        Return the dataset computed from the join of the
        2 datsets referenced by the given views
        """
        params = {
            'views': json.dumps(views),
            "join_type": join_type,
            "compute_key": compute_key
        }
        handle = self.context.get("/merge_join", params,
                                  "FE:: Error querying join on %s" % views)["handle"]
        return Dataset(self.context, handle)

    def get_blind_join(self, view_left, view_right, join_type, join_algo, join_key) -> Dataset:
        payload = {
            "view_left": view_left,
            "view_right": view_right,
            "join_type": join_type,
            "join_algo": join_algo,
            "join_key": join_key
        }
        handle = self.context.post(
            path="/blind_join",
            json=payload,
            params=None,
            error_message=f"Error querying join on {view_left}, {view_right}"
        )["handle"]

        return Dataset(self.context, handle)
