from .context import Context


class FHE():

    def __init__(self, context: Context):
        self.context = context

    def gen_key(self, vector_size, d, noise_deviation):
        """
        Generate a new key.
        """
        params = {
            "vector_size": vector_size,
            "d": d,
            "noise_deviation": noise_deviation,
        }
        handle = self.context.post("/fhe/gen_key", params,
                                   error_message=f"FHE:: Error generating key for n={vector_size}, d={d}")
        return handle

    def get_key(self, key_id):
        """
        Download the raw key
        """
        res = self.context.get(f"/fhe/key/{key_id}",
                               error_message=f"FHE:: Error fetching key for with id {key_id}")
        return res['key']

    def add_key(self, key, vector_size, d, noise_deviation):
        """
        Upload a new key
        """
        params = {
            "value_conf": {
                "vector_size": vector_size,
                "d": d,
                "noise_deviation": noise_deviation,
            },
            "key": key,
        }
        handle = self.context.post("/fhe/key", params,
                                   error_message=f"FHE:: Error uploading key")
        return handle

    def encrypt(self, key_id, data):
        """
        Encrypt a value with the given key
        """
        params = {
            "key_id": key_id,
            "data": data,
        }
        encrypted = self.context.post("/fhe/encrypt", params,
                                      error_message=f"FHE:: Error encrypting with key={key_id}, data={data}")
        return encrypted['res']

    def add(self, a, b):
        """
        Add two encrypted values
        """
        params = {
            "a": a,
            "b": b,
        }
        res = self.context.post("/fhe/add", params,
                                error_message=f"FHE:: Error computing {a} + {b}")['res']
        return res

    def rotate(self, v, n):
        """
        Rotate encrypted vector `v` by `n` elements
        """
        params = {
            "v": v,
            "n": n,
        }
        res = self.context.post("/fhe/rotate", params,
                                error_message=f"FHE:: Error rotating {v} by {n}")['res']
        return res

    def cmux_scal(self, bit, a, b):
        """
        Apply the `cmux_scal` operation.
        """
        params = {
            "current_bit": bit,
            "a": a,
            "b": b,
        }
        res = self.context.post("/fhe/cmux_scal", params,
                                error_message=f"FHE:: Error invoking `cmux_scal` for {bit}, {a}, {b}")['res']
        return res

    def decrypt(self, key_id, data):
        """
        Decrypt data with the given key
        """
        params = {
            "value": data,
            "key_id": key_id,
        }
        handle = self.context.post("/fhe/decrypt", params,
                                   error_message=f"FHE:: Error decrypting {data} with key {key_id}")
        return handle
