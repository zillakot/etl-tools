from pyspark.sql import SparkSession
from pyspark_extensions.helpers import test, init_logger

spark = SparkSession.Builder().appName('etl').master("local[4]").getOrCreate()

spark.sql('select 1').transform(test()).show()

logger = init_logger(spark, __name__)()
logger.info("info")
logger.warn("warn")
logger.error("error")