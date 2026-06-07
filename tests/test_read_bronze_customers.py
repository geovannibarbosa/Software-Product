from spark_session import get_spark
from pyspark.sql.functions import count

spark = get_spark("test-read-bronze")

bronze_path = "/opt/spark/lakehouse/bronze/customers"

df = spark.read.parquet(bronze_path)

# df.printSchema()
df.show(130, truncate=False)
df.groupBy("ingest_date").agg(count("*").alias("rows")).orderBy("ingest_date").show()

spark.stop()
