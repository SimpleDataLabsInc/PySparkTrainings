from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def SuppliersToCall(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("s_phone"), 
        col("s_acctbal"), 
        col("s_name"), 
        col("s_suppkey"), 
        col("ps_supplycost"), 
        col("ps_partkey"), 
        col("ps_suppkey")
    )
