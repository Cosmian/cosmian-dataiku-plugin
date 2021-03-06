# coding=utf-8
from dataiku.core import metrics, default_project_key
import json
from dataiku.core.intercom import jek_or_backend_void_call, backend_json_call, jek_or_backend_json_call

class Project():
    """
    This is a handle to interact with the current project

    Note: this class is also available as ``dataiku.Project``
    """

    def __init__(self, project_key=None):
        """Obtain a handle for a project

        :param str project_key: Project key, or None for the current project
        """
        self.project_key = project_key or default_project_key()

    def _repr_html_(self,):
        s = "Project[   <b>%s</b>   ]" % (self.project_key)
        return s

    # ################################### Metrics #############################

    def get_last_metric_values(self):
        """
        Get the set of last values of the metrics on this project, as a :class:`dataiku.ComputedMetrics` object
        """
        return metrics.ComputedMetrics(backend_json_call("metrics/projects/get-last-values", data = {
            "projectKey": self.project_key
        }))

    def get_metric_history(self, metric_lookup):
        """
        Get the set of all values a given metric took on this project
        :param metric_lookup: metric name or unique identifier
        """
        return backend_json_call("metrics/projects/get-metric-history", data = {
            "projectKey": self.project_key,
            "metricLookup" : metric_lookup if isinstance(metric_lookup, str) or isinstance(metric_lookup, unicode) else json.dumps(metric_lookup)
        })

    def save_external_metric_values(self, values_dict):
        """
        Save metrics on this project. The metrics are saved with the type "external"

        :param values_dict: the values to save, as a dict. The keys of the dict are used as metric names
        """
        return backend_json_call("metrics/projects/save-external-values", data = {
            "projectKey": self.project_key,
            "data" : json.dumps(values_dict)
        }, err_msg="Failed to save external metric values")

    def get_last_check_values(self):
        """
        Get the set of last values of the checks on this project, as a :class:`dataiku.ComputedChecks` object
        """
        return metrics.ComputedChecks(backend_json_call("checks/projects/get-last-values", data = {
            "projectKey": self.project_key
        }))

    def get_check_history(self, check_lookup):
        """
        Get the set of all values a given check took on this project
        :param check_lookup: check name or unique identifier
        """
        return backend_json_call("checks/projects/get-metric-history", data = {
            "projectKey": self.project_key,
            "checkLookup" : check_lookup if isinstance(check_lookup, str) or isinstance(check_lookup, unicode) else json.dumps(check_lookup)
        })

    def set_variables(self, variables):
        """
        Set all variables of the current project

        :param dict variables: must be a modified version of the object returned by get_variables
        """
        if "standard" not in variables:
            raise Exception("Missing 'standard' key in argument")
        if "local" not in variables:
            raise Exception("Missing 'local' key in argument")

        var_json = json.dumps(variables)
        jek_or_backend_void_call("variables/set-for-project?projectKey=" + self.project_key, data=var_json)

    def get_variables(self):
        """
        Get project variables
        :param bool typed: typed true to try to cast the variable into its original type (eg. int rather than string)

        Returns:
                A dictionary containing two dictionaries : “standard” and “local”. “standard” are regular variables, exported with bundles.
                “local” variables are not part of the bundles for this project
        """
        data = {"projectKey": self.project_key}
        return jek_or_backend_json_call("variables/get-for-project", data=data)

    def save_external_check_values(self, values_dict):
        """
        Save checks on this project. The checks are saved with the type "external"

        :param values_dict: the values to save, as a dict. The keys of the dict are used as check names
        """
        return backend_json_call("checks/projects/save-external-values", data = {
            "projectKey": self.project_key,
            "data" : json.dumps(values_dict)
        }, err_msg="Failed to save external check values")
        