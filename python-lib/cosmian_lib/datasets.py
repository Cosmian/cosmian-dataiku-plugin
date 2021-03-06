from .context import Context
from .dataset import Dataset
from .write_dataset import WriteDataset


class Datasets():

    def __init__(self, context: Context):
        self.context = context

    def retrieve(self, view_name) -> Dataset:
        """
        Retrieve a dataset from a view name for reading from it
        """
        handle = self.context.get(
            f"/view/{view_name}/raw_dataset/read_only")["handle"]
        return Dataset(self.context, handle)

    def write(self, view_name) -> WriteDataset:
        """
        Retrieve a dataset from a view name for writing to it
        """
        handle = self.context.get(
            f"/view/{view_name}/raw_dataset/read_only")["handle"]
        return Dataset(self.context, handle)

    def upload(self, schema, name) -> WriteDataset:
        """
        Create a new dataset to which rows can be uploaded to
        """
        handle = self.context.post(f"/dataset/create/{name}", schema)["handle"]
        return WriteDataset(self.context, handle)

    def delete(self, name):
        """
        Delete a dataset
        """
        self.context.delete(f"/dataset/{name}")
