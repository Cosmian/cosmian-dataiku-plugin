from .context import Context


class Dataset():

    def __init__(self, context: Context, handle: str):
        self.context = context
        self.handle = handle

    def schema(self):
        """
        Retrieve the schema of that dataset
        """
        schema = self.context.get("/dataset/%s/schema" % self.handle, None,
                                  error_message="dataset:: failed querying dataset: %s" % self.handle
                                  )
        response = []
        for col in schema["columns"]:
            response.append(
                {"name": col["name"], "type": cosmian_type_2_dataiku_type(col["data_type"])})
        return response

    def read_next_row(self):
        """
        Read the new row of the dataset.
        Returns an array of values, one per column.
        Returns None when the end of the dataset is reached.
        """
        return self.context.get("/dataset/%s/next" % self.handle, None,
                                error_message="dataset:: failed reading next row of dataset: %s" % self.handle,
                                allow_404=True
                                )


def cosmian_type_2_dataiku_type(ct):
    if ct == "hash":
        return "string"
    if ct == "int32":
        return "int"
    if ct == "int64":
        return "bigint"
    if ct == "float":
        return "double"
    if ct == "string" or ct == "hash" or ct == "blurred" or ct == "encrypted" or ct == "fe_cmp_encrypted":
        return "string"
    return "object"
