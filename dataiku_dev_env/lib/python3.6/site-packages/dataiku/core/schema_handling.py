import sys
from dataiku.core import base
from dateutil import parser as date_iso_parser
import numpy as np

def parse_iso_date(s):
    if s == "":
        return None
    else:
        return date_iso_parser.parse(s)

DKU_PANDAS_TYPES_MAP = {
    'int': np.int32,
    'bigint': np.int64,
    'float': np.float32,
    'double': np.float64,
    'boolean': np.bool
}

def str_to_bool(s):
    if s is None:
        return False
    return s.lower() == "true"

# used to stream typed fields in iter_tuples.
CASTERS = {
    "tinyint" : int,
    "smallint" : int,
    "int": int,
    "bigint": int,
    "float": float,
    "double": float,
    "date": parse_iso_date,
    "boolean": str_to_bool
}


PANDAS_DKU_TYPES_MAP = {
    'int64': 'bigint',
    'float64': 'double',
    'float32': 'float',
    'int32': 'int',
    'object': 'string',
    'int': 'int',
    'float': 'float',
    'bool': 'boolean',
    'datetime64[ns]': 'date',
    'Int64': 'bigint',
    'boolean': 'boolean'
}

def pandas_dku_type(dtype):
    '''Return the DSS type for a Pandas dtype'''
    from pandas.api.types import is_datetime64tz_dtype
    if is_datetime64tz_dtype(dtype):
        return 'date'
    else:
        return PANDAS_DKU_TYPES_MAP.get(str(dtype), "string")

def get_schema_from_df(df):
    ''' A simple function that returns a DSS schema from
    a Pandas dataframe, to be used when writing to a dataset
    from a data frame'''
    schema = []

    if len(set(df.columns)) != len(list(df.columns)):
        raise Exception("DSS doesn't support dataframes containing multiple columns with the same name.")

    for col_name in df.columns:
        # We convert the name of the column to a string to be able to insert it in the
        # schema object.
        # However, since Pandas 0.17, we keep the "pristine" name to lookup because you
        # can't anymore lookup 123 by "123"
        if not isinstance(col_name, base.dku_basestring_type):
            col_name_str = str(col_name)
        else:
            col_name_str = col_name

        column_type = {
            'name': col_name_str,
            'type': pandas_dku_type(df[col_name].dtype)
        }
        schema.append(column_type)
    return schema
