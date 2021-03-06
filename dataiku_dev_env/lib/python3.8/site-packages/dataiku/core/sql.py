
import csv
from dataiku.core.platform_exec import open_dku_stream

from dataiku.core import dataset, base, schema_handling, default_project_key
import os, json
import logging
from dataiku import Dataset
from dataiku.core.intercom import backend_stream_call, backend_void_call, backend_json_call, jek_json_call, jek_void_call
from dataiku.core import dkuio
from dataiku.core import flow

logger = logging.getLogger(__name__)

def _streamed_query_to_df(connection, query, pre_queries, post_queries, find_connection_from_dataset, db_type, extra_conf={}, infer_from_schema=False, parse_dates=True, bool_as_str=False, dtypes=None, script_steps=None, script_input_schema=None, script_output_schema=None):
    import pandas as pd
    data = {
        "connection" : connection,
        "query" : query,
        "preQueries" : json.dumps(pre_queries),
        "postQueries" : json.dumps(post_queries),
        "findConnectionFromDataset" : find_connection_from_dataset,
        "dbType" : db_type,
        "extraConf" : json.dumps(extra_conf),
        "scriptSteps" : json.dumps(script_steps) if script_steps is not None else None,
        "scriptInputSchema" : json.dumps(script_input_schema) if script_input_schema is not None else None,
        "scriptOutputSchema" : json.dumps(script_output_schema) if script_output_schema is not None else None,
        "projectKey" : default_project_key()
    }

    logger.info("Starting SQL query reader")
    # initiate the streaming (blocks until the database says it's ready to return values)
    streamingSession = backend_json_call("sql-queries/start-streaming",
        data = data)

    logger.info("Got initial SQL query response")

    queryId = streamingSession['queryId']

    # handle the special case of 'nothing to stream'
    if streamingSession['hasResults'] == False:
        return pd.DataFrame()

    parse_date_columns = None
    if infer_from_schema and "schema" in streamingSession:
        schema_columns = streamingSession["schema"]
        (inferred_names, inferred_dtypes, inferred_parse_date_columns) = Dataset.get_dataframe_schema_st(schema_columns, parse_dates=parse_dates, bool_as_str=bool_as_str)
        dtypes = inferred_dtypes
        parse_date_columns = inferred_parse_date_columns

    # fetch the data...
    resp_stream = backend_stream_call("sql-queries/stream",
        data = {"queryId": queryId}, err_msg="Query failed")
    # ... and stuff it (streamed) in a dataframe
    results = pd.read_table(resp_stream, sep='\t',
        doublequote=True,
        quotechar='"',
        dtype=dtypes,
        parse_dates=parse_date_columns)

    # query seems to have run fine. 'Seems'. Verify that.
    # note to self: this call has to be made after the dataframe creation, because it is streamed and the call
    # returns before the query is actually done
    backend_void_call("sql-queries/verify",
            data={"queryId" : queryId}, err_msg="Query failed")

    return results

class QueryReader:
    def __init__(self, connection, query, pre_queries=None, post_queries=None, find_connection_from_dataset=False, db_type='sql', extra_conf={}, timeOut=600000, script_steps=None, script_input_schema=None, script_output_schema=None):
        data = {
            "connection" : connection,
            "query" : query,
            "preQueries" : json.dumps(pre_queries),
            "postQueries" : json.dumps(post_queries),
            "findConnectionFromDataset" : find_connection_from_dataset,
            "timeOut" : timeOut,
            "dbType" : db_type,
            "extraConf" : json.dumps(extra_conf),
            "scriptSteps" : json.dumps(script_steps) if script_steps is not None else None,
            "scriptInputSchema" : json.dumps(script_input_schema) if script_input_schema is not None else None,
            "scriptOutputSchema" : json.dumps(script_output_schema) if script_output_schema is not None else None,
            "projectKey" : default_project_key()
        }
        logger.info("Starting SQL query reader")

        # initiate the streaming (blocks until the database says it's ready to return values)
        self.streamingSession = backend_json_call("sql-queries/start-streaming",
            data = data)

        logger.info("Got initial SQL query response")

    def get_schema(self):
        return self.streamingSession['schema']

    def iter_tuples(self, log_every=-1, no_header=False):
        def none_if_throws(f):
            def aux(*args, **kargs):
                try:
                    return f(*args, **kargs)
                except:
                    return None
            return aux

        if self.streamingSession['hasResults'] == False:
            for r in []:
                yield r   # barf. todo: find how to do that correctly
        else:
            queryId = self.streamingSession['queryId']
            # open the stream

            resp_stream = backend_stream_call("sql-queries/stream",
                    data={"queryId": queryId, "format" : "tsv-excel-noheader" if no_header else "tsv-excel-header"},
                    err_msg="Query failed")

            # prepare the casters for the columns
            casters = [
                schema_handling.CASTERS.get(col["type"], lambda s:s)
                for col in self.streamingSession['schema']
            ]
            # parse the csv stream
            count = 0
            for row_tuple in dkuio.new_utf8_csv_reader(resp_stream,
                                           delimiter='\t',
                                           quotechar='"',
                                           doublequote=True):
                if count == 0:
                    # first line is the header, skip
                    count = 1
                    continue
                yield [none_if_throws(caster)(val)
                       for (caster, val) in base.dku_zip_longest(casters, row_tuple)]
                count += 1
                if log_every > 0 and count % log_every == 0:
                    print ("Query - read %i lines" % (count))

            # query seems to have run fine. 'Seems'. Verify that.
            # note to self: this call has to be made after the dataframe creation, because it is streamed and the call
            # returns before the query is actually done
            backend_void_call("sql-queries/verify",
                data={"queryId" : queryId}, err_msg="Query failed")

class SQLExecutor:
    def __init__(self, connection):
        self.connection = connection

    def _stream(self, query):
        args = ['sql', self.connection, '-e', '%s ' % query]
        return open_dku_stream(args)

    def exec_query(self, query):
        import pandas as pd
        with self._stream(query) as dku_output:
            return pd.read_table(dku_output,
                                 sep='\t',
                                 doublequote=True,
                                 quotechar='"')


class SQLExecutor2:
    def __init__(self, connection=None, dataset=None):
        if connection and dataset:
            raise ValueError("only one of connection or dataset should be given")

        if dataset:
            if isinstance(dataset, Dataset):
                self._iconn = dataset.full_name
            else:
                self._iconn = Dataset(dataset).full_name
            self._find_connection_from_dataset = True
        else:
            self._iconn = connection
            self._find_connection_from_dataset = False

    @staticmethod
    def exec_recipe_fragment(output_dataset, query, pre_queries = [], post_queries = [],
                                    overwrite_output_schema=True,
                                    drop_partitioned_on_schema_mismatch=False):
        spec = flow.FLOW
        jek_void_call("sql/execute-partial-query-recipe",
            data={
                "outputDataset": output_dataset.full_name,
                "activityId" : spec["currentActivityId"],
                "query" : query,
                "preQueries" : json.dumps(pre_queries),
                "postQueries" : json.dumps(post_queries),
                "overwriteOutputSchema" : overwrite_output_schema,
                "dropPartitionedOnSchemaMismatch" : drop_partitioned_on_schema_mismatch
            }, err_msg="Query failed")

    def query_to_df(self, query, pre_queries=None, post_queries=None, extra_conf={}, infer_from_schema=False, parse_dates=True, bool_as_str=False, dtypes=None, script_steps=None, script_input_schema=None, script_output_schema=None):
        if not self._iconn:
            raise ValueError("SQLExecutor2 not configured: either 'connection' or 'dataset' are required (in SQLExecutor2 constructor)")
        return _streamed_query_to_df(self._iconn, query, pre_queries, post_queries, self._find_connection_from_dataset, "sql", extra_conf, infer_from_schema=infer_from_schema, parse_dates=parse_dates, bool_as_str=bool_as_str, dtypes=dtypes, script_steps=script_steps, script_input_schema=script_input_schema, script_output_schema=script_output_schema)

    def query_to_iter(self, query, pre_queries=None, post_queries=None, extra_conf={}, script_steps=None, script_input_schema=None, script_output_schema=None):
        if not self._iconn:
            raise ValueError("SQLExecutor2 not configured: either 'connection' or 'dataset' are required (in SQLExecutor2 constructor)")
        return QueryReader(self._iconn, query, pre_queries, post_queries, self._find_connection_from_dataset, "sql", extra_conf, script_steps, script_input_schema, script_output_schema)

class _HiveLikeExecutor:
    def __init__(self, dataset=None, database=None, connection=None, vtype=None):
        if connection and dataset or connection and database or database and dataset:
            raise ValueError("only one of connection, database or dataset should be given")

        if dataset:
            if isinstance(dataset, Dataset):
                self._vconn = dataset.full_name
            else:
                self._vconn = Dataset(dataset).full_name
            self._find_connection_from_dataset = True
        elif connection:
            self._vconn = "@virtual(%s):connection:%s" % (vtype, connection)
            self._find_connection_from_dataset = False
        elif database:
            self._vconn = "@virtual(%s):%s" % (vtype, database)
            self._find_connection_from_dataset = False
        else:
            self._vconn = None
            self._find_connection_from_dataset = None

        print ("Vconn = %s find=%s" % (self._vconn, self._find_connection_from_dataset))

class HiveExecutor(_HiveLikeExecutor):
    def __init__(self, dataset=None, database=None, connection=None):
        _HiveLikeExecutor.__init__(self, dataset, database, connection, vtype="hive-jdbc")

    @staticmethod
    def exec_recipe_fragment(query, pre_queries = [], post_queries = [], overwrite_output_schema=True,
                                    drop_partitioned_on_schema_mismatch=False, metastore_handling=None, extra_conf={}, add_dku_udf=False):
        spec = flow.FLOW
        jek_void_call("hive/execute-partial-recipe",
            data={
                "activityId" : spec["currentActivityId"],
                "query" : query,
                "preQueries" : json.dumps(pre_queries),
                "postQueries" : json.dumps(post_queries),
                "overwriteOutputSchema" : overwrite_output_schema,
                "dropPartitionedOnSchemaMismatch" : drop_partitioned_on_schema_mismatch,
                "metastoreHandling" : metastore_handling,
                "addDkuUdf" : add_dku_udf,
                "extraHiveConf" : json.dumps(extra_conf)
            }, err_msg="Query failed")

    def query_to_df(self, query, pre_queries=None, post_queries=None, extra_conf={}, infer_from_schema=False, parse_dates=True, bool_as_str=False, dtypes=None, script_steps=None, script_input_schema=None, script_output_schema=None):
        if not self._vconn:
            raise ValueError("HiveExecutor not configured: (one of database, connection or dataset required in constructor)")
        return _streamed_query_to_df(self._vconn, query, pre_queries, post_queries, self._find_connection_from_dataset, "hive-jdbc", extra_conf, infer_from_schema=infer_from_schema, parse_dates=parse_dates, bool_as_str=bool_as_str, dtypes=dtypes, script_steps=script_steps, script_input_schema=script_input_schema, script_output_schema=script_output_schema)

    def query_to_iter(self, query, pre_queries=None, post_queries=None, extra_conf={}, script_steps=None, script_input_schema=None, script_output_schema=None):
        if not self._vconn:
            raise ValueError("HiveExecutor not configured: (one of database, connection or dataset required in constructor)")
        return QueryReader(self._vconn, query, pre_queries, post_queries, self._find_connection_from_dataset, "hive-jdbc", extra_conf, script_steps, script_input_schema, script_output_schema)


class ImpalaExecutor(_HiveLikeExecutor):
    def __init__(self, dataset=None, database=None, connection=None):
        _HiveLikeExecutor.__init__(self, dataset, database, connection, vtype="impala-jdbc")

    @staticmethod
    def exec_recipe_fragment(output_dataset, query, pre_queries = [], post_queries = [], overwrite_output_schema=True, use_stream_mode=True):
        spec = flow.FLOW
        jek_void_call("impala/execute-partial-query-recipe",
            data={
                "outputDataset": output_dataset.full_name,
                "activityId" : spec["currentActivityId"],
                "query" : query,
                "preQueries" : json.dumps(pre_queries),
                "postQueries" : json.dumps(post_queries),
                "overwriteOutputSchema" : overwrite_output_schema,
                "useStreamMode" : use_stream_mode
            }, err_msg="Query failed")


    def query_to_df(self, query, pre_queries=None, post_queries=None, connection=None, extra_conf={}, infer_from_schema=False, parse_dates=True, bool_as_str=False, dtypes=None, script_steps=None, script_input_schema=None, script_output_schema=None):
        if not self._vconn:
            raise ValueError("HiveExecutor not configured: (one of database, connection or dataset required in constructor)")
        return _streamed_query_to_df(self._vconn, query, pre_queries, post_queries, self._find_connection_from_dataset, "impala-jdbc", extra_conf, infer_from_schema=infer_from_schema, parse_dates=parse_dates, bool_as_str=bool_as_str, dtypes=dtypes, script_steps=script_steps, script_input_schema=script_input_schema, script_output_schema=script_output_schema)

    def query_to_iter(self, query, pre_queries=None, post_queries=None, connection=None, extra_conf={}, script_steps=None, script_input_schema=None, script_output_schema=None):
        if not self._vconn:
            raise ValueError("HiveExecutor not configured: (one of database, connection or dataset required in constructor)")
        return QueryReader(self._vconn, query, pre_queries, post_queries, self._find_connection_from_dataset, "impala-jdbc", extra_conf, script_steps, script_input_schema, script_output_schema)
        
class SparkExecutor(_HiveLikeExecutor):
    def __init__(self, dataset=None, database=None, connection=None):
        _HiveLikeExecutor.__init__(self, dataset, database, connection, vtype="spark-livy")

    def query_to_df(self, query, pre_queries=None, post_queries=None, extra_conf={}, infer_from_schema=False, parse_dates=True, bool_as_str=False, dtypes=None, script_steps=None, script_input_schema=None, script_output_schema=None):
        if not self._vconn:
            raise ValueError("SparkExecutor not configured: (one of database, connection or dataset required in constructor)")
        return _streamed_query_to_df(self._vconn, query, pre_queries, post_queries, self._find_connection_from_dataset, "spark-livy", extra_conf, infer_from_schema=infer_from_schema, parse_dates=parse_dates, bool_as_str=bool_as_str, dtypes=dtypes, script_steps=script_steps, script_input_schema=script_input_schema, script_output_schema=script_output_schema)

    def query_to_iter(self, query, pre_queries=None, post_queries=None, extra_conf={}, script_steps=None, script_input_schema=None, script_output_schema=None):
        if not self._vconn:
            raise ValueError("SparkExecutor not configured: (one of database, connection or dataset required in constructor)")
        return QueryReader(self._vconn, query, pre_queries, post_queries, self._find_connection_from_dataset, "spark-livy", extra_conf, script_steps, script_input_schema, script_output_schema)

                