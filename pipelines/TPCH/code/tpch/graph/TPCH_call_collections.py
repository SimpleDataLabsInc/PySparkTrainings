from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def TPCH_call_collections(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder
    in0.write.format("delta").save("dbfs:/Prophecy/sparklearner123@gmail.com/TPCH_call_collections")
