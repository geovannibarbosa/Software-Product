from spark_session import get_spark

spark = get_spark("spark-test")

df = spark.range(1, 10)
df.show()

spark.stop()
