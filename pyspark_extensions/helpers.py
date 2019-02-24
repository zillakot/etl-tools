from pyspark.sql.functions import lit


def test():
    return lambda df: df.withColumn('test', lit('test'))


def init_logger(spark, name=__name__):
    def init():
        sc = spark.sparkContext
        logger = sc._jvm.org.apache.log4j
        log = logger.LogManager.getLogger(name)
        log.setLevel(logger.Level.WARN)
        return log
    return init
