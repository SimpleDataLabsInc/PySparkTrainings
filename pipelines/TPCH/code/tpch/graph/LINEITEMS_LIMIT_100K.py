from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tpch.config.ConfigStore import *
from tpch.udfs.UDFs import *

def LINEITEMS_LIMIT_100K(spark: SparkSession, in0: DataFrame):
    from pyspark.dbutils import DBUtils
    options = {
        "sfUrl": "https://HA30422.us-east-2.aws.snowflakecomputing.com",
        "sfUser": DBUtils(spark).secrets.get(scope = "anyademos", key = "username"),
        "sfPassword": DBUtils(spark).secrets.get(scope = "anyademos", key = "password"),
        "sfDatabase": "TPCH",
        "sfSchema": "DEMO",
        "sfWarehouse": ""
    }
    writer = in0.write.format("snowflake").options(**options)
    writer.option("dbtable", "LINEITEMS_LIMIT_100K").mode("overwrite").save()
