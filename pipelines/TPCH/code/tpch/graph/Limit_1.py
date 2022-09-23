from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def Limit_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.limit(100000)
