from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from exploretcph.config.ConfigStore import *
from exploretcph.udfs.UDFs import *
from prophecy.utils import *
from exploretcph.graph import *

def pipeline(spark: SparkSession) -> None:
    df_LINEITEM = LINEITEM(spark)
    df_Cleanup = Cleanup(spark, df_LINEITEM)
    df_AggQty = AggQty(spark, df_Cleanup)
    df_OrderBy = OrderBy(spark, df_AggQty)
    LINEITEM_AGG(spark, df_OrderBy)
    df_SQLStatement_1 = SQLStatement_1(spark, df_LINEITEM)
    df_Filter_2 = Filter_2(spark, df_SQLStatement_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "3328/pipelines/ExploreTCPH")
    MetricsCollector.start(spark = spark, pipelineId = "3328/pipelines/ExploreTCPH")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
