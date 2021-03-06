from ..context import Context


class Setup():
    """
        MCFE and DMCFE LWE setup parameters.
        Public Parameters are derived from these #see parameters()
    """
    clients: int
    message_length: int
    message_bound: int
    vectors_bound: int
    n0: int

    def __init__(self, clients: int, message_length: int, message_bound: int, vectors_bound: int, n0: int):
        """
        Create MCFE and DMCFE LWE setup parameters.
        A string can be used for the bounds since big numbers cannot fit in python integers
        """
        self.clients = clients
        self.message_length = message_length
        self.message_bound = message_bound
        self.vectors_bound = vectors_bound
        self.n0 = n0

    def to_json(self):
        return {
            "clients": self.clients,
            "message_length": self.message_length,
            "message_bound": str(self.message_bound),
            "vectors_bound": str(self.vectors_bound),
            "n0": self.n0
        }


class Parameters():
    clients: int
    message_length: int
    message_bound: int
    vectors_bound: int
    k: str
    q: str
    q0: str
    sigma: str
    n0: int
    m0: int

    def __init__(self, clients: int, message_length: int, message_bound: str, vectors_bound: str, k: str, q: str, q0: str, sigma: str, n0: int, m0: int):
        self.clients = clients
        self.message_length = message_length
        self.message_bound = message_bound
        self.vectors_bound = vectors_bound
        self.k = k
        self.q = q
        self.q0 = q0
        self.sigma = sigma
        self.n0 = n0
        self.m0 = m0


class SecretKey():
    coeffs: [[str]]

    def __init__(self, coeffs: [[str]]):
        self.coeffs = coeffs


class MasterSecretKey():
    coeffs: [[[str]]]

    def __init__(self, coeffs: [[[str]]]):
        self.coeffs = coeffs

    def client_secret_key(self, client_number) -> SecretKey:
        return SecretKey(self.coeffs[client_number])


class FunctionalKeyShare():
    coeffs: [str]

    def __init__(self, coeffs: [[str]]):
        self.coeffs = coeffs


class FunctionalKey():
    coeffs: [str]

    def __init__(self, coeffs: [str]):
        self.coeffs = coeffs


class MCFE():

    def __init__(self, context: Context):
        self.context = context

    def setup(self, clients: int, message_length: int, message_bound: int, vectors_bound: int, n0: int) -> Setup:
        """
        Create MCFE and DMCFE LWE setup parameters.
        `clients`: the number of clients
        `message_length`: m: the number of elements in the message vector
        `message_bound`: P: Message elements upper bound P i.e. x₁ ∈ {0,..., P-1}ᵐ where i∈{n}
        `vectors_bound`: V: Vectors elements upper bound V i.e. yᵢ ∈ {0,..., V-1}ᵐ where i∈{n}
        `n0`: ths size of the key
        """
        return Setup(clients, message_length, message_bound, vectors_bound, n0)

    def parameters(self, setup) -> [Parameters]:
        """
        Determine public parameters based on the setup parameters.
        Use #setup() to build a a Setup object.
        Returns the parameters used to encrypt messages
        in the DMCFE scheme
        """
        p = self.context.put("/mcfe/lwe/parameters", setup.to_json(), None,
                             "MCFE:: failed getting parameters for setup: %s" % setup
                             )
        return Parameters(
            clients=p["clients"],
            message_length=p["message_length"],
            message_bound=int(p["message_bound"]),
            vectors_bound=int(p["vectors_bound"]),
            k=int(p["k"]),
            q=int(p["q"]),
            q0=int(p["q0"]),
            sigma=int(p["sigma"]),
            n0=p["n0"],
            m0=p["m0"]
        )

    def fks_parameters(self, setup) -> [Parameters]:
        """
        Determine public parameters based on the setup parameters.
        Use #setup() to build a a Setup object.
        Returns the parameters used to encrypt messages functional key shares
        in the DMCFE scheme
        """
        p = self.context.put("/mcfe/lwe/fks_parameters", setup.to_json(), None,
                             "MCFE:: failed getting the FKS parameters for setup: %s" % setup
                             )
        return Parameters(
            clients=p["clients"],
            message_length=p["message_length"],
            message_bound=int(p["message_bound"]),
            vectors_bound=int(p["vectors_bound"]),
            k=int(p["k"]),
            q=int(p["q"]),
            q0=int(p["q0"]),
            sigma=int(p["sigma"]),
            n0=p["n0"],
            m0=p["m0"]
        )

    def create_lwe_master_secret_key(self, setup: Setup) -> str:
        """
        Create and save a MCFE LWE Master secret key.
        The key will be saved under the returned `uid`
        """
        return self.context.post("/mcfe/lwe/master_secret_key", {"setup": setup.to_json()}, None,
                                 "MCFE: failed creating an LWE MAster Secret Key with setup: %s" % setup)["uid"]

    def get_lwe_master_secret_key(self, uid: str) -> (Setup, MasterSecretKey):
        """
        Get an MCFE LWE Master secret key and associated setup using its `uid`
        Returns a tuple (Setup, MasterSecretKey)
        """
        res = self.context.get("/mcfe/lwe/master_secret_key/%s" % uid, None,
                               "MCFE: failed getting the LWE Master Secret Key with uid: %s" % uid)
        setup = Setup(
            clients=res["setup"]["clients"],
            message_length=res["setup"]["message_length"],
            message_bound=int(res["setup"]["message_bound"]),
            vectors_bound=int(res["setup"]["vectors_bound"]),
            n0=res["setup"]["n0"]
        )
        secret_key = MasterSecretKey(res["key"])
        return (setup, secret_key)

    def import_lwe_master_secret_key(self, setup: Setup, master_secret_key: MasterSecretKey) -> str:
        """
        Import and save an MCFE LWE Master secret key.
        The key will be saved under the returned `uid`
        """
        return self.context.post("/mcfe/lwe/master_secret_key/import", {"setup": setup.to_json(), "key": master_secret_key.coeffs}, None,
                                 "MCFE: failed importing an LWE Master Secret Key with setup: %s" % setup)["uid"]

    def update_lwe_master_secret_key(self, uid: str, setup: Setup, secret_key: SecretKey) -> str:
        """
        Update an existing DMCFE LWE Master secret key.
        Fails if the key does not exist.
        """
        return self.context.put("/mcfe/lwe/master_secret_key", {"uid": uid, "setup": setup.to_json(), "key": secret_key.coeffs}, None,
                                "MCFE: failed updating an existing LWE Master Secret Key with uid: %s and setup: %s" % (uid, setup))["uid"]

    def create_lwe_secret_key(self, setup: Setup) -> str:
        """
        Create and save a DMCFE LWE secret key.
        The key will be saved under the returned `uid`
        """
        return self.context.post("/mcfe/lwe/secret_key", {"setup": setup.to_json()}, None,
                                 "MCFE: failed creating an LWE Secret Key with setup: %s" % setup)["uid"]

    def get_lwe_secret_key(self, uid: str) -> (Setup, SecretKey):
        """
        Get a DMCFE LWE secret key and associated setup using its `uid`
        Returns a tuple (Setup, SecretKey)
        """
        res = self.context.get("/mcfe/lwe/secret_key/%s" % uid, None,
                               "MCFE: failed getting the LWE Secret Key with uid: %s" % uid)
        setup = Setup(
            clients=res["setup"]["clients"],
            message_length=res["setup"]["message_length"],
            message_bound=int(res["setup"]["message_bound"]),
            vectors_bound=int(res["setup"]["vectors_bound"]),
            n0=res["setup"]["n0"]
        )
        secret_key = SecretKey(res["key"])
        return (setup, secret_key)

    def import_lwe_secret_key(self, setup: Setup, secret_key: SecretKey) -> str:
        """
        Import and save a DMCFE LWE secret key.
        The key will be saved under the returned `uid`
        """
        return self.context.post("/mcfe/lwe/secret_key/import", {"setup": setup.to_json(), "key": secret_key.coeffs}, None,
                                 "MCFE: failed importing an LWE Secret Key with setup: %s" % setup)["uid"]

    def update_lwe_secret_key(self, uid: str, setup: Setup, secret_key: SecretKey) -> str:
        """
        Update an existing DMCFE secret key.
        Fails if the key does not exist.
        """
        return self.context.put("/mcfe/lwe/secret_key", {"uid": uid, "setup": setup.to_json(), "key": secret_key.coeffs}, None,
                                "MCFE: failed updating an existing LWE Secret Key with uid: %s and setup: %s" % (uid, setup))["uid"]

    def get_fks_lwe_secret_key(self, uid: str) -> (Setup, SecretKey):
        """
        Get an MCFE LWE Functional Key Share secret key and associated setup using its `uid`
        Returns a tuple (Setup, SecretKey)
        """
        res = self.context.get("/mcfe/lwe/fks_secret_key/%s" % uid, None,
                               "MCFE: failed getting the LWE Secret Key with uid: %s" % uid)
        setup = Setup(
            clients=res["setup"]["clients"],
            message_length=res["setup"]["message_length"],
            message_bound=int(res["setup"]["message_bound"]),
            vectors_bound=int(res["setup"]["vectors_bound"]),
            n0=res["setup"]["n0"]
        )
        secret_key = SecretKey(res["key"])
        return (setup, secret_key)

    def import_fks_lwe_secret_key(self, setup: Setup, secret_key: SecretKey) -> str:
        """
        Import and save a DMCFE Functional Key Share secret key.
        The key will be saved under the returned `uid`
        """
        return self.context.post("/mcfe/lwe/fks_secret_key/import", {"setup": setup.to_json(), "key": secret_key.coeffs}, None,
                                 "MCFE: failed importing an LWE FKS Secret Key with setup: %s" % setup)["uid"]

    def update_fks_lwe_secret_key(self, uid: str, setup: Setup, secret_key: SecretKey) -> str:
        """
        Update an existing DMCFE Functional Key Share secret key.
        Fails if the key does not exist.
        """
        return self.context.put("/mcfe/lwe/fks_secret_key", {"uid": uid, "setup": setup.to_json(), "key": secret_key.coeffs}, None,
                                "MCFE: failed updating an existing LWE Secret Key with uid: %s and setup: %s" % (uid, setup))["uid"]

    def create_lwe_functional_key(self, master_secret_key_uid: str, vectors: [[int]]) -> str:
        """
        Create and save a MCFE LWE functional key.
        The `uid` must be that of an existing Master Secret Key
        The passed `vectors` must have `number of clients` vectors of length equal to the message length
        The key will be saved under the returned `uid`
        """
        return self.context.post("/mcfe/lwe/functional_key", {"master_secret_key_uid": master_secret_key_uid, "vectors": vectors}, None,
                                 "MCFE: failed creating an LWE Functional Key with Master Secret Key: %s" % master_secret_key_uid)["uid"]

    def get_lwe_functional_key(self, uid: str) -> (Setup, FunctionalKey):
        """
        Get a MCFE LWE functional key and associated setup using its `uid`
        Returns a tuple (Setup, FunctionalKey)
        """
        res = self.context.get("/mcfe/lwe/functional_key/%s" % uid, None,
                               "MCFE: failed getting the LWE Functional Key with uid: %s" % uid)
        setup = Setup(
            clients=res["setup"]["clients"],
            message_length=res["setup"]["message_length"],
            message_bound=int(res["setup"]["message_bound"]),
            vectors_bound=int(res["setup"]["vectors_bound"]),
            n0=res["setup"]["n0"]
        )
        functional_key = FunctionalKey(res["key"])
        return (setup, functional_key)

    def create_lwe_functional_key_share(self, secret_key_uid: str, fks_secret_key_uid: str, vectors: [[int]], client: int) -> FunctionalKeyShare:
        """
        Issue a functional key share for the `vectors` where `client` is the index of our vector as a client.
        The `fks_secret_key` must have been issued with the other clients
        so that `∑ fks_skᵢ = 0` where `i ∈ {n}` and `n` is the number of clients.
        The passed `vectors` must have `number of clients` vectors of length equal to the message length.

        Calculated as `fksᵢ = Enc₂(fks_skᵢ, yᵢ.sk, ᵢ, H(y))` where `i` is this client number,
        `fks_skᵢ` is the functional key share secret key, `sk` is the secret key and `yᵢ` is the
        vector for that client
        """
        return FunctionalKeyShare(
            self.context.post("/mcfe/lwe/functional_key/share",
                              {"secret_key_uid": secret_key_uid,
                               "fks_secret_key_uid": fks_secret_key_uid,
                               "vectors": convert_vectors_to_str(vectors),
                               "client": client
                               },
                              None,
                              "DMCFE: failed generating an LWE Functional key share with secret key uid: %s and FKS secret key uid: %s" % (secret_key_uid, fks_secret_key_uid))
        )

    def recover_functional_key(self, setup: Setup, functional_key_shares: [FunctionalKeyShare], vectors: [[int]]) -> str:
        """
        Recover a functional key from the key shares sent by the clients. All clients must have
        provided their share.
        Returns the `uid` of the newly recovered Functional key
        """
        fks_array = []
        for fks in functional_key_shares:
            fks_array.append(fks.coeffs)
        return self.context.put("/mcfe/lwe/functional_key/recover",
                                {"setup": setup.to_json(),
                                 "functional_key_shares": fks_array,
                                 "vectors": convert_vectors_to_str(vectors)},
                                None,
                                "DMCFE: failed recovering an LWE Functional key")["uid"]

    def import_lwe_functional_key(self, setup: Setup, secret_key: SecretKey) -> str:
        """
        Import and save a DMCFE Functional Key.
        The key will be saved under the returned `uid`
        """
        return self.context.post("/mcfe/lwe/functional_key/import", {"setup": setup.to_json(), "key": secret_key.coeffs}, None,
                                 "MCFE: failed importing an LWE FKS Secret Key with setup: %s" % setup)["uid"]

    def encrypt(self, uid: str, labeled_messages: dict) -> dict:
        """
        Encrypt the provided labeled messages using the MCFE Secret Key with the given `uid`.

        Labeled messages should be a dictionary with a string label as key and an integer as values

            `'{"label_1": [1,2,3], "label_2": [465467898754654, 789879878798798987987988787, 1]}'`

        Returns a dictionary of labeled cipher texts. Each cipher texts is an array of hex strings.
        """
        # python supports arbitrarily long integer by default so convert everything to string
        # as it might exceed the 2^64 limit on integer API side
        lm = {}
        for label, m in labeled_messages.items():
            message = []
            for v in m:
                message.append(str(v))
            lm[label] = message
        encryption_request = {"uid": uid, "labeled_messages": lm}
        return self.context.post("/mcfe/lwe/encrypt", encryption_request, None,
                                 "MCFE: failed encrypting the messages using LWE Secret Key with uid: %s" % uid)

    def decrypt(self, functional_key_uid: str, labeled_cipher_texts: dict, vectors: [[]]) -> dict:
        # FIXME: figure out a way to deduplicate documentation between REST documentation and python
        # documentation. Right now this is a copy paste of the REST documentation
        """
        Decrypt the provided messages using the MCFE Functional Key at `uid` and corresponding vectors.

        The `labeled_cipher_texts` should be a dictionary with the string labels as key, and arrays
        of arrays of hex strings as values. The length of the outer array should be identical to the number of
        clients. The length of the inner array should be identical to the length of the clear text messages.

        The vectors is a 2 dimensional array of integers or radix-10 numbers enclosed in strings i.e.

                `[[1,2,3],[4,5,6]] or [["123","980989897687668768686","42"], [7,8,9]]`

        The first dimension length should be identical to the number of clients, the second dimension length
        to that of the length of a clear text message.

        The function returns a dictionary of labels and corresponding decrypted value as a radix 10 number
        in a string. Labels will NOT be in the same order as the one provided in the labeled cipher texts
        dictionary
        """
        decryption_request = {"functional_key_uid": functional_key_uid,
                              "labeled_cipher_texts": labeled_cipher_texts,
                              "vectors": convert_vectors_to_str(vectors)}
        as_string = self.context.post("/mcfe/lwe/decrypt",
                                      decryption_request,
                                      None,
                                      "MCFE: failed decrypting the cipher texts with the given vectors and LWE Functional Key with uid: %s" % functional_key_uid)
        # python supports arbitrarily long integer by default
        res = {}
        for k, v in as_string.items():
            res[k] = int(v)
        return res

    def create_lwe_fks_secret_keys(self, setup: Setup) -> [SecretKey]:
        """
        Create a set of secret keys used by clients to encrypt the functional key shares.
        These keys sum to zero in ℤq. This utility will be replaced by an MPC protocol.
        """
        json_keys = self.context.post("/mcfe/lwe/fks_secret_keys", setup.to_json(), None,
                                      "MCFE: failed creating a set of FKS LWE Secret Keys with setup: %s" % setup)
        fks_keys = []
        for json_key in json_keys:
            fks_keys.append(SecretKey(json_key))
        return fks_keys


def convert_vectors_to_str(vectors: [[]]) -> [[str]]:
    """
    python supports arbitrarily long integer by default so convert everything to string
    as it might exceed the 2^64 limit on integer API side
    """
    str_vec = []
    for client_v in vectors:
        str_client_vec = []
        for v in client_v:
            str_client_vec.append(str(v))
        str_vec.append(str_client_vec)
    return str_vec
