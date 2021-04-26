from .context import Context
import time

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


MPC_LEADER_PLAYERS_QUEUED =  "PlayersQueued"
MPC_PLAYER_FINISHED =  "Finished"


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

    def queue(self, mpc_program, players, computation_id=None, output_view=None) -> int:
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
            'player_number': 0,
        }

        payload = {
            "mpc_program": mpc_program,
            # "data": data,
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

    def accept(self, computation_id, input_data, output_view=None):
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

    def get_leader_computation(self,computation_id):
        """
        Returns the status of the computation and of the players
        as seen from the leader. Returns 404 if this player is not the
        leader for that computation
        """
        return self.context.get(f"/mpc/leader/{computation_id}",
                                error_message="Mpc::failed getting the computation from the leader"
                                )

    def wait_for_leader_status(self, leader_computation_id: str, state:str , seconds: int = 60):
        """
        Wait for the leader to reach a certain state
        before returning

        The parameter `seconds` set the maximim waiting time and defaults to 60
        A ValueError is raised in case of timeout
        """
        msg = f"waiting max {seconds} seconds for leader status '{state}' on computation {leader_computation_id}"
        print(msg)
        timeout = seconds
        while True:
            st = self.get_leader_computation(leader_computation_id)['state']
            if st == state or state in st :
                st = str(st).replace("\\n", "\n")
                print(f"... done in {seconds-timeout} seconds with status {st}")
                return
            if timeout ==  0:
                msg = f"Timout {msg}, status is {self.get_leader_computation(leader_computation_id)['state']}".replace("\\n", "\n")
                print(msg)
                raise ValueError(msg)
            timeout -= 1
            time.sleep(1)


    def wait_for_player_status(self, computation_id: str, state: str, seconds: int = 60) ->str:
        """
        Wait for the player to reach a certain state
        before returning

        The parameter `seconds` set the maximim waiting time and defaults to 60
        A ValueError is raised in case of timeout
        """
        msg = f"waiting max {seconds} seconds for player status '{state}' on computation {computation_id}"
        print(msg)
        timeout = seconds
        while True:
            st = self.status()['queue'][computation_id]['state']
            if st == state or (isinstance(st, dict) and state in st) :
                n_str = str(st).replace("\\n", "\n")
                print(f"... done in {seconds-timeout} seconds with status {n_str}")
                if isinstance(st, dict):
                    return st[state]
                return None
            if timeout ==  0:
                msg = f"Timout {msg}, status is {self.status()['queue'][computation_id]['state']}".replace("\\n", "\n")
                print(msg)
                raise ValueError(msg)
            timeout -= 1
            time.sleep(1)

def mpc_quick_run(
    git_url: str,
    commit_hash: str,
    data = [[],[],[]],
    mpc_runtimes = [
        {
            'server_name': 'localhost',
            'rest_port': 10000,
        },
        {
            'server_name': 'localhost',
            'rest_port': 10001,
        },
        {
            'server_name': 'localhost',
            'rest_port': 10002,
        },
    ]
):
    """
    A utility to quickly run a computation on a set of MPC runtimes

    This is useful to quickly test an MPC program from the command line 
    without going through the orchestrator UIs.

    Tge `data` parameter is a list of input data tables' one per participant.
    It defaults to a list of 3 empty input data tables 

    The `mpc_runtimes` parameter defaults to a list of 
    {
        'server_name': SERVER_NAME,
        'rest_port': REST_PORT,
    }
    which are those of the 3 participants of the CipherCompute EAP developer version
    see: https://github.com/Cosmian/CipherCompute

    """
    servers = [MPC(Context(f"http://{rt['server_name']}:{rt['rest_port']}")) for rt in mpc_runtimes]
    computation_id = "quick_run"
    try:
        program = {
            'repository': git_url,
            'revision': commit_hash,
        }
        # use participant 0 as the leader
        servers[0].queue(program, mpc_runtimes, computation_id)
        servers[0].wait_for_leader_status(computation_id,state=MPC_LEADER_PLAYERS_QUEUED)
        for index, server in enumerate(servers):
            server.accept(computation_id, data[index])
        servers[0].wait_for_player_status(computation_id,state=MPC_PLAYER_FINISHED)

        status = servers[0].status()['queue'][computation_id]
        return {
            'debug_output': status['debug_output'],
            'data': [server.results(computation_id) for server in servers]
        }
    except ValueError as e:
        raise e
    except BaseException as e:
        status = servers[0].status()['queue'][computation_id]
        print(status['debug_output'])
        raise e
    finally:
        for server in servers:
            try:
                server.dequeue(computation_id)
            finally:
                pass
