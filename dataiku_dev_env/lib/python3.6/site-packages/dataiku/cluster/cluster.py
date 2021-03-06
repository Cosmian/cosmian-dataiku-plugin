import os, json

class Cluster(object):
    """The base interface for a Python Cluster"""

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config
        
    def get_start_progress_target(self):
        """
        If the cluster will return some progress info during the start() action, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None
        
    def get_stop_progress_target(self):
        """
        If the cluster will return some progress info during the stop() action, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def start(self, progress_callback):
        """
        Make the cluster operational in DSS, creating an actual cluster if necessary.
        
        :param progress_callback: a function expecting 1 value: current progress

        :returns: a tuple of : 
                  * the settings needed to access hadoop/hive/impala/spark on the cluster
                  * an dict of data to pass to to other methods when handling the cluster created
        """
        raise Exception("unimplemented")

    def stop(self, data, progress_callback):
        """
        Stop the cluster
        
        :param data: the dict of data that the start() method produced for the cluster
        :param progress_callback: a function expecting 1 value: current progress
        """
        raise Exception("unimplemented")
