from .context import Context
from .dataset import Dataset


class Datasets():

    def __init__(self, context: Context):
        self.context = context

    def retrieve(self, view_name, sort_ds) -> Dataset:
        """
        Retrieve a dataset from a view name
        """
        dataset_sort_path = "sorted_dataset" if sort_ds else "raw_dataset"
        handle = self.context.get("/view/%s/%s" %
                                  (view_name, dataset_sort_path))["handle"]
        return Dataset(self.context, handle)
