from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def JoinSuppliersPartsupp(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.s_suppkey") == col("in1.ps_suppkey")), "inner")\
        .where((col("in0.reject_record") == lit(False)))
