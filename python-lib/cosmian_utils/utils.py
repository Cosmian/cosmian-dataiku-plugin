# DSS automatically add this file as a module
# In your IDE it should be sym-linked to libs ot the python Env.


def recover_datasets_info(datasets):
    url = ''
    views = []
    # process the list of datasets
    # collect the views which will be passed as parameter to the call
    # views must all be from the same server
    for dataset in datasets:
        config = dataset.get_config()['params']['customConfig']
        view = config['view_name']
        # sorted = config['sorted']
        server_url = config['server_url']
        if not server_url.endswith("/"):
            server_url += "/"
        if url == '':
            url = server_url
        else:
            if url != server_url:
                raise ValueError(
                    "All datasets must be accessed from the same Cosmian server")
        views.append(view)
    return url, views


def cosmian_schema_2_dataiku_schema(schema):
    di_schema = []
    for col in schema["columns"]:
        di_schema.append(
            {"name": col["name"], "type": cosmian_type_2_dataiku_type(col["data_type"])})
    return di_schema


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
