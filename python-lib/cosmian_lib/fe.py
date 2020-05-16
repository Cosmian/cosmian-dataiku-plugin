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
        handle = self.context.get("merge_join", params,
                                  "FE:: Error querying join on %s" % views)["handle"]
        return Dataset(self.context, handle)
