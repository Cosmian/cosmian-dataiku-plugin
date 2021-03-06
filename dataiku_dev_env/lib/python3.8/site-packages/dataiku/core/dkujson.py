from __future__ import print_function
import json
import numpy as np
import sys
# import pandas


class DKUJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.float64):
            return json.JSONEncoder.default(self, float(obj))
        elif isinstance(obj, np.ndarray) and obj.ndim == 1:
            return obj.tolist()
        elif isinstance(obj, np.generic):
            return obj.item()
        #elif isinstance(obj, pandas.DataFrame):
        #    return json.loads(obj.to_json(orient="records"))
        else:
            return json.JSONEncoder.default(self, obj)


def set_default_decorator(fn, **param):
    def wrapped(*args, **kvargs):
        kvargs.update(param)
        return fn(*args, **kvargs)
    return wrapped

dump = set_default_decorator(json.dump, cls=DKUJSONEncoder)
dumps = set_default_decorator(json.dumps, cls=DKUJSONEncoder)
load = json.load
loads = json.loads


def load_from_filepath(filepath):
    try:
        with open(filepath) as fp:
            return load(fp)
    except ValueError as e:
        print(e, file=sys.stderr)
        return None


def dump_to_filepath(filepath, obj):
    """Write human readable json

    We first serialize the object
    to avoid corrupting the file
    if the object is not serializable."""
    obj_json = dumps(obj, indent=4)
    with open(filepath, 'w') as f:
        f.write(obj_json)


def dump(f, obj):
    """Write human readable json

    We first serialize the object
    to avoid corrupting the file
    if the object is not serializable."""
    obj_json = dumps(obj, indent=4)
    f.write(obj_json)
