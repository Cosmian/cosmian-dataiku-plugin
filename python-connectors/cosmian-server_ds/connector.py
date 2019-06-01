# This file is the actual code for the custom Python dataset cosmian-server_ds

# import the base class for the custom dataset
from __future__ import print_function
from __future__ import print_function
from dataiku.connector import Connector
import requests

# import json
# from os import sys

"""
A custom Python dataset is a subclass of Connector.

The parameters it expects and some flags to control its handling by DSS are
specified in the connector.json file.

Note: the name of the class itself is not relevant.
"""


class CosmianDatasetConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        # perform some more initialization
        self.server_url = str(self.config.get("server_url"))
        if ~self.server_url.endswith("/"):
            self.server_url += "/"
        self.dataset_name = str(self.config.get("dataset_name"))
        # attempt to create open a source to this dataset
        headers = {
            "Accept-Encoding": "gzip",
            "Accept": "application/json"
        }
        params = {}
        try:
            self.session = requests.Session()
            r = self.session.get(
                url="%sdataset/%s/source" % (self.server_url, self.dataset_name),
                params=params,
                headers=headers
            )
            if r.status_code != 200:
                raise ValueError("Cosmian Server:: Error querying dataset: %s, status code: %s, reason :%s" % (
                    self.dataset_name, r.status_code, r.text))
            resp = r.json()
            self.handle = resp["handle"]
        except requests.ConnectionError:
            raise ValueError("Failed establishing connection to Cosmian Server at: %s" % self.server_url)

    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.

        The returned schema may be None if the schema is not known in advance.
        In that case, the dataset schema will be infered from the first rows.

        If you do provide a schema here, all columns defined in the schema
        will always be present in the output (with None value),
        even if you don't provide a value in generate_rows

        The schema must be a dict, with a single key: "columns", containing an array of
        {'name':name, 'type' : type}.

        Example:
            return {"columns" : [ {"name": "col1", "type" : "string"}, {"name" :"col2", "type" : "float"}]}

        Supported types are: string, int, bigint, float, double, date, boolean
        """

        # In this example, we don't specify a schema here, so DSS will infer the schema
        # from the columns actually returned by the generate_rows method
        # return None

        # attempt to establish simple connection to the root URL
        headers = {
            "Accept-Encoding": "gzip",
            "Accept": "application/json"
        }
        params = {}
        try:
            r = self.session.get(
                url="%ssource/%s/schema" % (self.server_url, self.handle),
                params=params,
                headers=headers
            )
            if r.status_code != 200:
                raise ValueError("Cosmian Server:: Error querying dataset: %s, status code: %s, reason :%s" % (
                    self.dataset_name, r.status_code, r.text))
            schema = r.json()
            response = {"columns": []}
            for col in schema:
                response["columns"].append({"name": col["name"], "type": cosmian_type_2_dataiku_type(col["data_type"])})
            return response
            # return {"columns": [{"name": "col1", "type": "string"}, {"name": "col2", "type": "float"}]}
        except requests.ConnectionError:
            raise ValueError("Failed connecting to Cosmian Server at: %s" % self.server_url)

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                      partition_id=None, records_limit=-1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """

        headers = {
            "Accept-Encoding": "gzip",
            "Accept": "application/json"
        }
        params = {}
        i = 0
        go_on = True
        while (i < records_limit) & go_on:
            try:
                r = self.session.get(
                    url="%ssource/%s/next" % (self.server_url, self.handle),
                    params=params,
                    headers=headers
                )
                if r.status_code == 404:  # EOF
                    break
                if r.status_code == 200:
                    yield r.json()
                else:
                    raise ValueError("Cosmian Server:: Error querying dataset: %s, status code: %s, reason :%s" % (
                        self.dataset_name, r.status_code, r.text))

                # return {"columns": [{"name": "col1", "type": "string"}, {"name": "col2", "type": "float"}]}
            except requests.ConnectionError:
                raise ValueError("Failed querying Cosmian Server at: %s" % self.server_url)

    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                   partition_id=None):
        """
        Returns a writer object to write in the dataset (or in a partition).

        The dataset_schema given here will match the the rows given to the writer below.

        Note: the writer is responsible for clearing the partition, if relevant.
        """
        raise Exception("Unimplemented")

    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise Exception("Unimplemented")

    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")

    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")


class CustomDatasetWriter(object):
    def __init__(self):
        pass

    def write_row(self, row):
        """
        Row is a tuple with N + 1 elements matching the schema passed to get_writer.
        The last element is a dict of columns not found in the schema
        """
        raise Exception("unimplemented")

    def close(self):
        pass

    # Int32,
    # Int64,
    # FixedPoint,
    # Float,
    # String,


def cosmian_type_2_dataiku_type(ct):
    if ct == "hash":
        return "string"
    if ct == "int32":
        return "int"
    if ct == "int64":
        return "int"
    if ct == "float":
        return "float"
    if ct == "string":
        return "string"
    return "object"
