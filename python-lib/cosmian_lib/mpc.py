from .context import Context


class MpcExecution():
    def __init__(self, context):
        self.context = context
        self.row_id = 0

    def push(self, data):
        """
        Upload a raw MPC vector to the MPC program
        """
        MPC(self.context).push(data)

    def next(self):
        """
        Fetch the next data row from the MPC program.
        This will block until data is actually available.
        """
        row_id = self.row_id
        self.row_id += 1
        return MPC(self.context).row(row_id)

    def stop(self):
        """
        Stop the MPC instance and destroy the handle.
        """
        MPC(self.context).stop()
        self.context = None


class MPC():

    def __init__(self, context: Context):
        self.context = context

    def output_format(self, output_format):
        """
        Conversion from MPC array output to field/column names.
        """
        self.context.post("/mpc/output_format", output_format,
                          error_message="Mpc::failed to set output format"
                          )

    def create_program(self, name, code):
        """
        Upload a new mpc program in `mamba` source file format.
        The name chosen can later be used in `start` to choose the program for execution.
        The upload needs to be done only once, the program can then be used in later computations.
        """
        self.context.post(f"/mpc/program/{name}", {'source': code},
                          error_message="Mpc::failed to upload program"
                          )

    def update_program(self, name, code):
        """
        Update an mpc program in `mamba` source file format.
        Use this to change a program without first deleting it and creating it again.
        """
        self.context.put(f"/mpc/program/{name}", {'source': code},
                         error_message="Mpc::failed to upload program"
                         )

    def delete_program(self, name):
        """
        Delete a program from the server.
        """
        self.context.delete(f"/mpc/program/{name}",
                            error_message="Mpc::failed to delete program"
                            )

    def download_program(self, name):
        """
        Download the MPC program's SCALE assembly in binary format.
        """
        self.context.get(f"/mpc/program/{name}",
                         error_message="Mpc::failed to download MPC program assembly"
                         )

    def results(self, handle):
        """
        Returns all available results
        """
        return self.context.get(f"/mpc/{handle}/results",
                                error_message="Mpc::failed to get mpc result"
                                )

    def finished(self, handle):
        """
        Returns whether the MPC program has finished running
        """
        return self.context.get(f"/mpc/{handle}/finished",
                                error_message="Mpc::failed to query mpc `finished` flag"
                                )

    def result(self, handle, idx):
        """
        Returns the result with the given index
        """
        return self.context.get(f"/mpc/{handle}/result/{idx}",
                                error_message="Mpc::failed to get mpc result"
                                )

    def status(self):
        """
        Some meta information about the current MPC operation. Returns `None` if no MPC operation
        is running
        """
        return self.context.get("/mpc/status",
                                error_message="Mpc::failed to get mpc status"
                                )

    def stop(self):
        """
        Hard force the current MPC node to be terminated. This will cause all connected MPC nodes
        to emit errors.
        """
        return self.context.get("/mpc/stop",
                                error_message="Mpc::failed to shut down mpc"
                                )

    def dequeue(self, handle):
        """
        Delete the queue entry from the local queue. Does not affect connected players beyond
        the fact that the computation gets shut down if running.
        """
        return self.context.delete(f"/mpc/{handle}",
                                   error_message="Mpc::failed to remove mpc queue entry"
                                   )

    def queue(self, program, data, players, computation_id=None, output_view=None) -> MpcExecution:
        """
        Enqueue a computation and send out requests to the other players to accept
        the computation.
        """

        setup = {
            'lsss': {
                'threshold': 1,
                'modp': '146994499793943626591367124308987067351',
            },
            'players': players,
            'my_index': 0,
        }

        payload = {
            "program": program,
            "data": data,
            "output_view": output_view,
            "setup": setup
        }

        if computation_id is None:
            return self.context.post(f"/mpc/queue", payload,
                                     error_message="Mpc::failed to enqueue program"
                                     )
        else:
            return self.context.post(f"/mpc/{computation_id}", payload,
                                     error_message="Mpc::failed to enqueue program"
                                     )

    def accept(self, computation_id, input_data, output_view=None) -> MpcExecution:
        """
        Accept the computation with the given id to be run whenever the system
        is ready.
        """

        payload = {
            "local_id": computation_id,
            "data": input_data,
            "output_view": output_view,
        }

        return self.context.post(f"/mpc/accept", payload,
                                 error_message="Mpc::failed to accept program"
                                 )
