from datetime import datetime

class ComputedMetrics(object):
    """
    Handle to the metrics of a DSS object and their last computed value
    """
    def __init__(self, raw):
        self.raw = raw

    def get_metric_by_id(self, metric_id):
        """
        Retrive the info for a given metric
        
        :param metric_id: unique identifier of the metric
        """
        all_ids = []
        for metric in self.raw["metrics"]:
            all_ids.append(metric["metric"]["id"])
            if metric["metric"]["id"] == metric_id:
                return metric
        raise Exception("Metric %s not found among: %s" % (metric_id, all_ids))

    def get_global_data(self, metric_id):
        """
        Get the global value point of a given metric, or throws.
        
        For a partitioned dataset, the global value is the value of the metric computed
        on the whole dataset (coded as partition 'ALL').
        
        :param metric_id: unique identifier of the metric
        """
        for partition_data in self.get_metric_by_id(metric_id)["lastValues"]:
            # for folders, partition is not defined
            if "partition" not in partition_data or partition_data["partition"] == "" or partition_data["partition"] == "NP" or partition_data["partition"] == "ALL":
                return partition_data
        raise Exception("No data found for global partition for metric %s" % metric_id)

    def get_global_value(self, metric_id):
        """
        Get the global value of a given metric, or throws.
        
        For a partitioned dataset, the global value is the value of the metric computed
        on the whole dataset (coded as partition 'ALL').
        
        :param metric_id: unique identifier of the metric
        """
        return ComputedMetrics.get_value_from_data(self.get_global_data(metric_id))

    def get_partition_data(self, metric_id, partition):
        """
        Get the value point of a given metric for a given partition, or throws.
        
        :param metric_id: unique identifier of the metric
        :param partition: partition identifier
        """
        for partition_data in self.get_metric_by_id(metric_id)["lastValues"]:
            if partition_data["partition"] == partition:
                return partition_data

    def get_partition_value(self, metric_id, partition):
        """
        Get the value of a given metric for a given partition, or throws.
        
        :param metric_id: unique identifier of the metric
        :param partition: partition identifier
        """
        return ComputedMetrics.get_value_from_data(self.get_partition_data(metric_id, partition))

    def get_first_partition_data(self, metric_id):
        """
        Get a value point of a given metric, or throws. The first value encountered is returned.
        
        :param metric_id: unique identifier of the metric
        """
        for partition_data in self.get_metric_by_id(metric_id)["lastValues"]:
            return partition_data
        raise Exception("No partition data for metric %s" % metric_id)

    def get_partition_data_for_version(self, metric_id, version_id):
        """
        Get for a metric the first partition matching version_id
        :param metric_id: unique identifier of the metric
        :param version_id: unique identifier of the version
        :return:
        """
        for partition_data in self.get_metric_by_id(metric_id)["lastValues"]:
            if partition_data["partition"] == version_id:
                return partition_data
        raise Exception("No partition with value data for metric %s" % metric_id)

    def get_all_ids(self):
        """
        Get the identifiers of all metrics defined in this object
        """
        all_ids = []
        for metric in self.raw["metrics"]:
            all_ids.append(metric["metric"]["id"])
        return all_ids


    @staticmethod
    def get_value_from_data(data):
        """
        Retrieves the value from a metric point, cast in the appropriate type (str, int or float).
        
        For other types, the value is not cast and left as a string.
        
        :param data: a value point for a metric, retrieved with :func:`dataiku.ComputedMetrics.get_global_data` or  :func:`dataiku.ComputedMetrics.get_partition_data`
        """
        dtype = data.get("dataType", "STRING")
        if dtype in ["BIGINT", "INT"]:
            return int(data["value"])
        elif dtype in ["FLOAT", "DOUBLE"]:
            return float(data["value"])
        else:
            return data["value"]
            
            
class MetricDataPoint(object):
    """
    A value of a metric, on a partition
    """
    def __init__(self, raw):
        self.raw = raw

    def get_metric(self):
        """
        Returns the metric as a JSON object
        """
        return self.raw['metric']

    def get_metric_id(self):
        """
        Returns the metric's id
        """
        return self.raw['metric'].get('id', None)

    def get_partition(self):
        """
        Returns the partition on which the value was computed
        """
        return self.raw['partition']

    def get_value(self):
        """
        Returns the value of the metric, as a string
        """
        return self.raw['value']

    def get_compute_time(self):
        """
        Returns the time at which the value was computed
        """
        return datetime.utcfromtimestamp((int)(self.raw['time'] / 1000))

    def get_type(self):
        """
        Returns the type of the value
        """
        return self.raw['type']

    def __repr__(self,):
        return self.get_metric_id() + ' on ' + self.get_partition() + ' / ' + self.get_compute_time().strftime('%Y-%m-%d %H:%M:%S') + ' = ' + self.get_value() + ' (' + self.get_type() + ')'
        
class ComputedChecks(object):
    """
    Handle to the checks of a DSS object and their last computed value
    """
    def __init__(self, raw):
        self.raw = raw

    def get_check_by_name(self, check_name):
        """
        Retrive the info for a given check
        
        :param check_name: unique identifier of the check
        """
        all_names = []
        for check in self.raw["checks"]:
            all_names.append(check["name"])
            if check["name"] == check_name:
                return check
        raise Exception("Check %s not found among: %s" % (check_name, all_names))

    def get_global_data(self, check_name):
        """
        Get the global value point of a given check, or throws.
        
        For a partitioned dataset, the global value is the value of the check computed
        on the whole dataset (coded as partition 'ALL').
        
        :param check_name: unique identifier of the check
        """
        for partition_data in self.get_check_by_name(check_name)["lastValues"]:
            # for folders, partition is not defined
            if "partition" not in partition_data or partition_data["partition"] == "" or partition_data["partition"] == "NP" or partition_data["partition"] == "ALL":
                return partition_data
        raise Exception("No data found for global partition for check %s" % check_name)

    def get_global_value(self, check_name):
        """
        Get the global value of a given check, or throws.
        
        For a partitioned dataset, the global value is the value of the check computed
        on the whole dataset (coded as partition 'ALL').
        
        :param check_name: unique identifier of the check
        """
        return ComputedChecks.get_outcome_from_data(self.get_global_data(check_name))

    def get_partition_data(self, check_name, partition):
        """
        Get the value point of a given check for a given partition, or throws.
        
        :param check_name: unique identifier of the check
        :param partition: partition identifier
        """
        for partition_data in self.get_check_by_name(check_name)["lastValues"]:
            if partition_data["partition"] == partition:
                return partition_data

    def get_partition_value(self, check_name, partition):
        """
        Get the value of a given check for a given partition, or throws.
        
        :param check_name: unique identifier of the check
        :param partition: partition identifier
        """
        return ComputedChecks.get_outcome_from_data(self.get_partition_data(check_name, partition))

    def get_first_partition_data(self, check_name):
        """
        Get a value point of a given check, or throws. The first value encountered is returned.
        
        :param check_name: unique identifier of the check
        """
        for partition_data in self.get_check_by_name(check_name)["lastValues"]:
            return partition_data
        raise Exception("No partition data for check %s" % check_name)

    def get_all_names(self):
        """
        Get the identifiers of all checks defined in this object
        """
        all_names = []
        for check in self.raw["checks"]:
            all_names.append(check["name"])
        return all_names


    @staticmethod
    def get_outcome_from_data(data):
        """
        Retrieves the value from a check point, cast in the appropriate type (str, int or float).
        
        For other types, the value is not cast and left as a string.
        
        :param data: a value point for a check, retrieved with :func:`dataiku.ComputedChecks.get_global_data` or  :func:`dataiku.ComputedChecks.get_partition_data`
        """
        return data["outcome"]
            
            
class CheckDataPoint(object):
    """
    A value of a check, on a partition
    """
    def __init__(self, raw):
        self.raw = raw

    def get_check(self):
        """
        Returns the check as a JSON object
        """
        return self.raw['check']

    def get_partition(self):
        """
        Returns the partition on which the value was computed
        """
        return self.raw['partition']

    def get_value(self):
        """
        Returns the value of the check, as a string
        """
        return self.raw['outcome']

    def get_compute_time(self):
        """
        Returns the time at which the value was computed
        """
        return datetime.utcfromtimestamp((int)(self.raw['time'] / 1000))

    def __repr__(self,):
        return self.get_check_name() + ' on ' + self.get_partition() + ' / ' + self.get_compute_time().strftime('%Y-%m-%d %H:%M:%S') + ' = ' + self.get_value()
                