from ..context import Context


class Keypair():
    private_key_uid: str
    public_key_uid: str

    def __init__(self, private_key_uid: str, public_key_uid: str):
        self.private_key_uid = private_key_uid
        self.public_key_uid = public_key_uid


class DSum():

    def __init__(self, context: Context):
        self.context = context

    @staticmethod
    def modulus():
        return 2 ** 252 + 27742317777372353535851937790883648493

    def create_key_pair(self) -> Keypair:
        """
        Create a Curve 25519 key pair (256 bits) in the internal KMS which is required
        to participate as a client to a DSum.
        Returns the secret key and public key identifiers.
        The public key must be exported and provided to the other clients participating
        to the DSum
        """
        res = self.context.post(
            "/dsum/create_key_pair", None, None, "DSum: failed creating a Curve 25519 Keypair")
        return Keypair(res['private_key_id'], res['public_key_id'])

    def get_private_key(self, uid: str) -> str:
        """
        Retrieve a (256 bits) Curve 25519 private key from its uid and
        return the bytes as an hex string
        """
        return self.context.get(
            "/dsum/private_key/%s" % uid, None, "DSum: failed retrieving the Curve 25519 private key with uid: %s" % uid)['key']

    def get_public_key(self, uid: str) -> str:
        """
        Retrieve a (256 bits) Curve 25519 public key from its uid and
        return the bytes as an hex string
        """
        return self.context.get(
            "/dsum/public_key/%s" % uid, None, "DSum: failed retrieving the Curve 25519 private key with uid: %s" % uid)['key']

    def import_public_key(self, hex_bytes: str) -> str:
        """
        Import a Curve 25519 public key, its bytes represented as an hex string
        Returns the uid of the imported key
        """
        return self.context.post(
            "/dsum/public_key", {"key": hex_bytes}, None, "DSum: failed importing a Curve 25519 public key")['uid']

    def update_public_key(self, uid: str, hex_bytes: str) -> str:
        """
        Update an existing Curve 25519 public key, its bytes represented as an hex string
        Returns the uid of the updated key
        """
        return self.context.put(
            "/dsum/public_key", {"uid": uid, "key": hex_bytes}, None,
            "DSum: failed updating the Curve 25519 public key with uid: %s" % uid)['uid']

    def secret_share(self, client_number: int, private_key_uid: str, public_key_uids: [str], label: str, value_to_share: int) -> str:
        """
        Create a Secret share of a (distributed) sum
        All clients participating to the sum must use the same 'label'
        The public and private keys must be generated using the provided facilities in the `DSum`
        API. These keys are Curve 25519 256 bit keys. They must all be available in the KMS.
        The uid in the `pubic_keys` array at index `client_number` must be the public key uid
        of this client.
        The value to share and the sum are in Zₚ where p is the modulus of the 25519 curve;
        its value can be retrieved calling `modulus()`
        Returns the (encrypted) share
        """
        return self.context.post(
            "/dsum/secret_share",
            {
                "client_number": client_number,
                "private_key_uid": private_key_uid,
                "public_keys_uid_s": public_key_uids,
                "label": label,
                "value_to_share": str(value_to_share),
            },
            None,
            "DSum: failed secret sharing the value: %d, for client: %d" % (
                value_to_share, client_number)
        )['share']

    def recombine(self, secret_shares: [str]) -> int:
        """
        Request to recombine secret shares provided by the different clients.
        The shares are byte arrays encoded as hex strings
        This operation simply decode the bytes as Big Integer assuming they are big endian
        then add them all together modulo the order of the Curve 25519
        """
        return int(self.context.put(
            "/dsum/recombine",
            {
                "secret_shares": secret_shares,
            },
            None,
            "DSum: failed recombining the secret shares")
            ['sum'])
