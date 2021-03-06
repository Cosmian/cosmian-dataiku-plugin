import multiprocessing.dummy as mp  # uses threads instead of full processes
from .context import Context


class CKKS():

    def __init__(self, context: Context):
        self.context = context

    #
    # Helpers
    #
    def encrypt_download(self, uid, to_encrypt: list, filepath: str) -> str:
        """
        Encrypt and then download data to a file.
        Return the full filepath the file has been written to
        """
        # encrypt
        print("Encrypting data with context uid: %s" % uid)
        #start_time = time.perf_counter()
        uid_enc = self.provider_encrypt(uid, to_encrypt)
        #end_time = time.perf_counter()
        #print("Encryption took", end_time - start_time, "seconds.")
        assert uid_enc is not None
        print("Success encrypting data with uid: %s" % uid_enc)
        # now perform download
        print("Downloading %s..." % filepath)
        #start_time = time.perf_counter()
        num_bytes = self.provider_download_data(uid_enc, filepath)
        #end_time = time.perf_counter()
        #print("Downloading took", end_time - start_time, "seconds.")
        print("Success downloading provider enc data with new uid: %s (%d bytes)" %
              (uid_enc, num_bytes))
        return uid_enc

    def access_lut(self, uid, kwh_prices, days):
        def lut_proc(day):
            # encode kwh price
            #start_time = time.perf_counter()
            encoded_kwh_prices = self.encode(
                uid, kwh_prices[day].flatten().tolist())
            #end_time = time.perf_counter()
            #print("Day", day, "- Encoding took", end_time - start_time, "seconds.")
            assert encoded_kwh_prices is not None

            # compute lut
            #start_time = time.perf_counter()
            data = self.lut(uid, encoded_kwh_prices)
            #end_time = time.perf_counter()
            #print("Day", day, "- LUT took", end_time - start_time, "seconds.")
            assert data is not None
            return data

        # i = 50
        # while i > 0:
        #     print(i)
        #     i -= 1
        #     datas = list()
        #     for day in range(days):
        #         datas.append(lut_proc(day))

        with mp.Pool() as p:
            datas = p.map(lut_proc, range(days))
        return datas

    #
    # API
    #
    def provider_init(self) -> str:
        """
        Setup crypto primitives.
        Return the uid corresponding to crypto context.
        """
        return self.context.get("/ckks/provider/init", None,
                                "CKKS:: failed init crypto"
                                )

    def provider_encrypt(self, uid, input_vec) -> str:
        """
        Encrypt input vector, return the uid specific to this encrypted data.
        To be called after `provider_init`
        """
        params = {
            'input': input_vec,
        }
        return self.context.post("/ckks/provider/encrypt/%s" % uid, params,
                                 "CKKS:: failed encrypting data on provider side"
                                 )

    def provider_decrypt(self, uid):
        """
        Decrypt the data corresponding to the uid
        """
        return self.context.get("/ckks/provider/decrypt/%s" % uid, None,
                                "CKKS:: failed decrypting data for uid: %s" % uid
                                )

    def valoconso(self, power_option_uid, power_consum_uid, subscr_uid, encoded_power_table, encoded_subscr_table) -> str:
        """
        Compute the ValoConso given parameters,
        from Provider side:
        - encrypted power option uuid
        - encrypted power consumptions uuid
        - encrypted subscription option uuid
        and from Computation side:
        - encoded table of power prices
        - encoded table of subscription prices
        Return uid of result
        """
        params = {
            'power_uuid': power_option_uid,
            'consumption_uuid': power_consum_uid,
            'subscr_uuid': subscr_uid,
            'power_table': encoded_power_table,
            'subscr_table': encoded_subscr_table,
        }
        return self.context.post("/ckks/computation/valoconso", params,
                                 "CKKS:: failed ValoConso computation"
                                 )["success"]

    def encode(self, uid, input_vec):
        """
        Encode the cleartext data into polynomials, using crypto context identified by `uid`.
        Return encoded version of `input_vec`, as a vector of bytes.
        """
        params = {
            'input': input_vec,
        }
        return self.context.post("/ckks/computation/encode/%s" % uid, params,
                                 "CKKS:: failed encoding data for uid: %s" % uid
                                 )["data"]

    def encode_multi(self, uid, input_vec_vec):
        """
        Encode a vector of cleartext vector of data into polynomials, using crypto context identified by `uid`.
        Return encoded version of all elements in `input_vec`, as a vector of vector of bytes.
        """
        params = {
            'input': input_vec_vec,
        }
        return self.context.post("/ckks/computation/encode_multi/%s" % uid, params,
                                 "CKKS:: failed multi encode of data for uid: %s" % uid
                                 )["data"]

    def lut(self, uid, encoded):
        """
        Proceed to blind access in a LUT.
        Multiply encoded vector with encrypted on identified by `uid`, and then sum the result.
        Return a vector of ciphertexts
        """
        params = {
            'data': encoded,
        }
        return self.context.post("/ckks/computation/lut/%s" % uid, params,
                                 "CKKS:: failed LUT for uid: %s" % uid
                                 )["data"]

    def loop_mul_add(self, uid, enc_data, steps=None):
        """
        Execute some computations over two vectors of ciphertexts.
        Loop over data and process each element with:
        - a rotation
        - a multiplication
        - an addition
        Return the resulting vector of ciphertexts
        """
        params = {
            'data_input': enc_data,
            'steps': steps,
        }
        return self.context.post("/ckks/computation/loop_mul_add/%s" % uid, params,
                                 "CKKS:: failed loop Rot/Mul/Add on data for uid: %s" % uid
                                 )["data"]

    def strip_add(self, uid, subscriber_fee, kwh_conso) -> str:
        """
        Add two ciphertexts and strip data.
        Return uid of result
        """
        params = {
            'bytes_ct_1': subscriber_fee,
            'bytes_ct_2': kwh_conso,
        }
        return self.context.post("/ckks/computation/strip_add/%s" % uid, params,
                                 "CKKS:: failed Strip/Add on data for uid: %s" % uid
                                 )["success"]

    def provider_download_data(self, uid: str, data_file: str) -> int:
        """
        Download to the given data_file,
        the provider data generated during the provider set-up for the given uid.
        Returns the number of bytes downloaded.
        """
        return self.context.download("/ckks/provider/data/%s" % uid, data_file, None, None,
                                     "CKKS:: failed downloading provider data for uid: %s" % uid
                                     )

    def computation_download_data(self, uid: str, data_file: str) -> int:
        """
        Download to the given data_file,
        the computed data generated during the processing for the given uid.
        Returns the number of bytes downloaded.
        """
        return self.context.download("/ckks/computation/data/%s" % uid, data_file, None, None,
                                     "CKKS:: failed downloading computed data for uid: %s" % uid
                                     )

    def provider_load_data(self, data_file: str) -> str:
        """
        Load data in provider side to be decrypted, return the uid.
        This is a synchronous call and may take a few minutes to return
        """
        return self.context.upload("/ckks/provider/setup", data_file, None, None, None,
                                   "CKKS:: failed loading provider data"
                                   )

    def computation_load_data(self, data_file: str) -> str:
        """
        Load data in computation side to be processed, return the uid.
        This is a synchronous call and may take a few minutes to return
        """
        return self.context.upload("/ckks/computation/setup", data_file, None, None,
                                   "CKKS:: failed loading computed data"
                                   )

    def delete_uid(self, uid):
        """
        Delete data corresponding to the uid
        """
        return self.context.delete("/ckks/delete/%s" % uid, None,
                                   "CKKS:: failed deleting data for uid: %s" % uid
                                   )

    def delete_all(self):
        """
        Delete all CKKS data
        """
        return self.context.delete("/ckks/all", None,
                                   "CKKS:: failed deleting all the CKKS data"
                                   )

    def square(self, uid) -> str:
        """
        Square the data corresponding to the uid
        """
        return self.context.get("/ckks/computation/square/%s" % uid, None,
                                "CKKS:: failed squaring data for uid: %s" % uid
                                )["success"]

    def mul(self, uid, encoded) -> str:
        """
        Multiply one encrypted data corresponding to the uid with given encoded data
        """
        params = {
            'data': encoded,
        }
        return self.context.post("/ckks/computation/mul/%s" % uid, params,
                                 "CKKS:: failed multiplying data for uid: %s" % uid
                                 )["success"]

    def sum(self, uid) -> str:
        """
        Sum the encrypted data corresponding to the uid
        """
        return self.context.get("/ckks/computation/sum/%s" % uid, None,
                                "CKKS:: failed summing up data for uid: %s" % uid
                                )["success"]
