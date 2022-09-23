from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def CallCollections(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    df1 = in0.filter(((col("refer_collections") == lit(True)) & (col("ps_supplycost") > lit(1000))))
    df2 = in0.filter(((col("refer_collections") == lit(True)) & (col("ps_supplycost") < lit(1000))))

    return df1, df2
