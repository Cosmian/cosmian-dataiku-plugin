from .context import Context


class WriteDataset():

    def __init__(self, context: Context, handle: str):
        self.context = context
        self.handle = handle

    def write_next_row(self, row):
        """
        Write the new row of the dataset.
        """
        self.context.post(f"/dataset/{self.handle}/push", row,
                          error_message=f"dataset:: failed writing next row of dataset: {self.handle}",
                          )
