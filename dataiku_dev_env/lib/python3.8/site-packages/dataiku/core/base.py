import sys
import os
import re

from dataiku.base.utils import safe_unicode_str
from dataiku.core import flow
from dataiku.base import remoterun
import itertools

if sys.version_info > (3,0):
    dku_basestring_type = str
    dku_zip_longest = itertools.zip_longest
else:
    dku_basestring_type = basestring
    dku_zip_longest = itertools.izip_longest


def get_shared_secret():
    with open('%s/shared-secret.txt' % get_dip_home(), 'r') as fp:
        secret = fp.read()
        return secret.strip()

def set_dip_home(dip_home):
    remoterun.set_dku_env_var_and_sys_env_var('DIP_HOME', dip_home)

def get_dip_home():
    return remoterun.get_env_var('DIP_HOME')

def is_container_exec():
    return os.environ.get('DKU_CONTAINER_EXEC', '0') == '1'


class Computable(object):

    def _init_data_from_flow(self, obj_type, project_key):
        if self.lookup is None:
            raise Exception("No identifier given for %s" % obj_type)

        self.readable = False
        self.writable = False

        # Flow mode, initialize partitions to read and write and read/write flags
        if flow.FLOW is not None:
            for flow_input in flow.FLOW["in"]:
                if flow_input["smartName"] == self.lookup or flow_input["fullName"] == self.lookup or flow_input.get("boxLabel", None) == self.lookup or flow_input.get("modelLabel", None) == self.lookup:
                    self.readable = True
                    self.spec_item = flow_input
                    self.name = flow_input["fullName"]
                    if "partitions" in flow_input:
                        self.read_partitions = flow_input["partitions"]
            for flow_output in flow.FLOW["out"]:
                if flow_output["smartName"] == self.lookup or flow_output["fullName"] == self.lookup or flow_output.get("boxLabel", None) == self.lookup or flow_output.get("modelLabel", None) == self.lookup:
                    self.name = flow_output["fullName"]
                    self.writable = True
                    self.spec_item = flow_output
                    if "partition" in flow_output:
                        self.writePartition = flow_output["partition"]
            if not self.readable and not self.writable:
                raise Exception("%s %s cannot be used : declare it as input or output of your recipe" % (obj_type, self.lookup))
            (self.project_key, self.short_name) = self.name.split(".", 1)


    @property
    def full_name(self,):
        return self.project_key + "." + self.short_name


    def set_write_partition(self,spec):
        """Sets which partition of the dataset gets written to when
        you create a DatasetWriter. Setting the write partition is
        not allowed in Python recipes, where write is controlled by
        the Flow."""
        if flow.FLOW is not None and self.ignore_flow == False:
            raise Exception("You cannot explicitly set partitions when "
                            "running within Dataiku Flow")
        self.writePartition = spec

    def add_read_partitions(self, spec):
        """Add a partition or range of partitions to read.

        The spec argument must be given in the DSS partition spec format.
        You cannot manually set partitions when running inside
        a Python recipe. They are automatically set using the dependencies.
        """
        if flow.FLOW is not None and self.ignore_flow == False:
            raise Exception("You cannot explicitly set partitions when "
                            "running within Dataiku Flow")
        if self.read_partitions is None:
            self.read_partitions = []
        self.read_partitions.append(spec)


# See com.dataiku.dip.partitioning.PartitioningUtils
class PartitionEscaper:

    ONLY_ALPHANUM = "[^0-9a-zA-Z]"
    ONLY_ALPHANUM_DASHES = "[^0-9a-zA-Z-]"

    PYTHON3 = sys.version_info >= (3,0,0)

    @staticmethod
    def escape(partition_name, keep=ONLY_ALPHANUM):
        clean_partition_name = safe_unicode_str(partition_name)

        def str_to_ord(s):
            if isinstance(s, str):  # for py 2
                return map(ord, s)
            elif isinstance(s, bytes):  # for py 3
                return s
            else:  # should never happen
                return list()

        def escape_char(match):
            match_str = match.group().encode("utf8")
            return safe_unicode_str("".join("_{:02x}".format(c) for c in str_to_ord(match_str)))

        return re.sub(keep, escape_char, clean_partition_name)

    @staticmethod
    def unescape(partition_name):
        DECODE = "(?:_[0-9a-f]{2})+"
        clean_partition_name = safe_unicode_str(partition_name)

        def unescape_char(match):
            match_str = match.group()
            ret = bytearray([int("0x" + hex_str, 0) for hex_str in match_str.split("_")[1:]]).decode("utf8")
            return safe_unicode_str(ret)

        return re.sub(DECODE, unescape_char, clean_partition_name)

    @staticmethod
    def build_partition_id(partition_names):
        """
            Builds partition id using proper encoding

            ex: (female, C) => "female|C"

            :param partition_names: str or iterable(str)
            :return: str (py3) or unicode (py2)
        """
        if isinstance(partition_names, dku_basestring_type) or isinstance(partition_names, bytes):
            return safe_unicode_str(partition_names)
        elif hasattr(partition_names, "__iter__"):
            return safe_unicode_str(u"|".join(safe_unicode_str(p) for p in partition_names))
        else:  # should not happen
            return safe_unicode_str(partition_names)
