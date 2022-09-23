from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from exploretcph.config.ConfigStore import *
from exploretcph.udfs.UDFs import *
from prophecy.utils import *
from exploretcph.graph import *

def pipeline(spark: SparkSession) -> None:
    df_LINEITEM = LINEITEM(spark)
    df_AggQty = AggQty(spark, df_LINEITEM)
    df_OrderBy = OrderBy(spark, df_AggQty)
    LINEITEM_AGG(spark, df_OrderBy)

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
