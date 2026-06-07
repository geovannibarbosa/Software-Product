from spark_session import get_spark

def main():
    spark = get_spark("test-read-silver")

    silver_base_path = "/opt/spark/lakehouse/silver"

    tables = ["shipments"]

    for table in tables:
        print(f"\n=== Reading silver.{table} ===")

        path = f"{silver_base_path}/{table}"

        df = spark.read.parquet(path)

        df.printSchema()
        df.show(130, truncate=False)

        print(f"Total rows: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
