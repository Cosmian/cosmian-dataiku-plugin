import json
from .context import Context
from .dataset import Dataset


class MPC_OLD():

    def __init__(self, context: Context):
        self.context = context

    def run_ip_mpc(self, views):
        params = {
            'views': json.dumps(views),
            "join_type": "recurring",  # FIXME this will have to go after the OVH demo
        }
        handle = self.context.get("/merge_join", params,
                                  error_message="mpc:: failed to run mpc on %s" % views)["handle"]
        return Dataset(self.context, handle)

    def run_linear_regression(self, views, s_mode, columns, range_start, range_end):
        if s_mode == 'stack':
            mode = 'aggregate_datasets'
        else:
            mode = 'split_dimensions'
        params = {
            'views': json.dumps(views),
            'columns': json.dumps(columns),
            'mode': mode,
            'range_start': range_start,
            'range_end': range_end,
        }
        print("Linear regression params: ", params)
        handle = self.context.get("/linear_regression", params,
                                  error_message="mpc:: failed to run mpc on %s" % views
                                  )["handle"]
        return Dataset(self.context, handle)
