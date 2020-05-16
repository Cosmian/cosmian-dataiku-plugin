from .server import Server


class MpcExecution():
    def __init__(self, servers):
        self.servers = servers
        self.row_id = [0 for _ in servers]

    def push(self, player_id, data):
        self.servers[player_id].context.post("/mpc/push", data,
                                             error_message="Mpc::failed to push mpc data"
                                             )

    def next(self, player_id):
        row_id = self.row_id[player_id]
        self.row_id[player_id] += 1
        return self.servers[player_id].context.get("/mpc/row/%d" % row_id,
                                                   error_message="Mpc::failed to get mpc row %d" % row_id
                                                   )


class Mpc():
    def __init__(self, players):
        """
        Begin scale setup on the server, you can do this immediately after calling
        `set_players`. It's going to take some time on the server, so you can configure
        programs afterwards and only invoke `start` once you're ready. This way you're
        not waiting for the setup phase to finish anymore once you're ready to start.
        """
        if len(players) < 3:
            raise ValueError(
                "Cannot do MPC with less than 3 players, but used %d" % players.len())
        self.servers = []
        for player in players:
            self.servers.append(
                Server("http://%s:%d/" % (player.get('server_name'), player.get('rest_port'))))
        self.setup = {
            'run_id': 'current_run',
            'lsss': {
                'threshold': 1,
                'modp': '146994499793943626591367124308987067351',
            },
            'players': players,
        }
        if len(self.servers) < 3:
            raise ValueError(
                "Cannot do MPC with less than 3 players, but used %d" % self.servers.len())
        self.params = []
        for (i, server) in enumerate(self.servers):
            params = server.context.post("/mpc/generic_setup/%d" % i, self.setup,
                                         error_message="Mpc::failed to setup mpc"
                                         )
            self.params.append(params)

    def start(self, code, input_format, output_format) -> MpcExecution:
        """
        Start the mpc computation. Do not use this `Mpc` instance anymore after calling `start`.
        """
        programs = {
            'restart': 'restart()\n',
            'main': code,
        }

        for (i, server) in enumerate(self.servers):
            for (name, code) in programs.items():
                server.context.post("/mpc/code/%s" % name, {'source': code},
                                    error_message="Mpc::failed to upload program"
                                    )
            mpc_start = {
                'run_id': 'current_run',
                'programs': ['restart', 'main'],
                'params': self.params[i]
            }
            server.context.post("/mpc/start/%d" % i, mpc_start,
                                error_message="Mpc::failed to start mpc"
                                )
            server.context.post("/mpc/output_format", output_format,
                                error_message="Mpc::failed to set output format"
                                )
            server.context.post("/mpc/input_format", input_format,
                                error_message="Mpc::failed to set output format"
                                )

        mpc = MpcExecution(self.servers)
        # Make self unusable
        self.servers = None
        self.setup = None
        return mpc
