from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def ReplaceSpaces(spark: SparkSession, in0: DataFrame) -> DataFrame:
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
        col("l_linestatus"), 
        col("l_shipdate"), 
        col("l_commitdate"), 
        col("l_receiptdate"), 
        regexp_replace(col("l_shipinstruct"), " ", "_").alias("l_shipinstruct"), 
        regexp_replace(col("l_shipmode"), " ", "_").alias("l_shipmode")
    )
