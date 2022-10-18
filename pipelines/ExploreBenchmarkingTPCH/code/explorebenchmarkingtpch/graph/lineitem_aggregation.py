from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from explorebenchmarkingtpch.config.ConfigStore import *
from explorebenchmarkingtpch.udfs.UDFs import *

def lineitem_aggregation(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder
    in0.write\
        .format("delta")\
        .option("overwriteSchema", True)\
        .mode("overwrite")\
        .save("dbfs:/Prophecy/sparklearner123@gmail.com/lineitem_aggregation")
