from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def RejectRecords(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("s_suppkey"), 
        col("s_name"), 
        col("s_address"), 
        col("s_nationkey"), 
        regexp_replace(col("s_phone"), "-", "").alias("s_phone"), 
        col("s_acctbal"), 
        col("s_comment"), 
        when(((length(col("s_suppkey")) < lit(2)) | col("s_nationkey").isNull()), lit("true"))\
          .otherwise(lit("false"))\
          .alias("reject_record"), 
        expr("if((s_acctbal < 0), 'true', 'false')").alias("refer_collections")
    )
