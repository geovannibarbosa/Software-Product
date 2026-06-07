from pyspark.sql import SparkSession


def get_spark(app_name: str = "ecommerce-lakehouse"):
    """
    Cria e retorna uma SparkSession padronizada
    para todos os jobs do projeto.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )

    return spark
