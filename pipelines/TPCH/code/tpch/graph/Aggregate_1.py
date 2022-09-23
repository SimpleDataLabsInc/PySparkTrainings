from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def Aggregate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("l_returnflag"), col("l_linestatus"))

    return df1.agg(
        sum(col("l_quantity")).alias("sum_qty"), 
        sum(col("l_extendedprice")).alias("sum_base_price"), 
        sum(col("l_extendedprice") * lit(1) - col("l_discount")).alias("sum_disc_price"), 
        sum(col("l_extendedprice") * lit(1) - col("l_discount") * lit(1) + col("l_tax")).alias("sum_charge"), 
        avg(col("l_quantity")).alias("avg_qty"), 
        avg(col("l_extendedprice")).alias("avg_price"), 
        avg(col("l_discount")).alias("avg_disc"), 
        count(lit(1)).alias("count_order")
    )
