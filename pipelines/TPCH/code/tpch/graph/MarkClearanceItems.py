from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def MarkClearanceItems(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("l_orderkey"), 
        col("l_partkey"), 
        col("l_suppkey"), 
        col("l_linenumber"), 
        col("l_quantity"), 
        col("l_extendedprice"), 
        col("l_discount"), 
        col("l_tax"), 
        col("l_returnflag"), 
        col("l_shipdate"), 
        col("l_receiptdate"), 
        col("l_shipinstruct"), 
        when(((col("l_discount") > lit(20)) | col("l_returnflag").eqNullSafe(lit(True))), lit("true"))\
          .otherwise(lit("false"))\
          .alias("l_clearanceStatus")
    )
