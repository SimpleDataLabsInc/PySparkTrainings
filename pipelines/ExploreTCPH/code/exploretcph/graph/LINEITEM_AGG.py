from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from exploretcph.config.ConfigStore import *
from exploretcph.udfs.UDFs import *

def LINEITEM_AGG(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder
    in0.write\
        .format("delta")\
        .option("overwriteSchema", True)\
        .mode("overwrite")\
        .save("dbfs:/Prophecy/sparklearner123@gmail.com/lineitem_agg/")
