from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DecimalType, TimestampType)

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_ts", TimestampType()),
    StructField("status", StringType()),
    StructField("payment_method", StringType()),
    StructField("total_amount", DecimalType(10, 2)),
    StructField("currency", StringType()),
    StructField("ingestion_ts", TimestampType()),
    StructField("ingest_date", StringType()),
    StructField("source_file", StringType())
])

order_items_schema = StructType([
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(10, 2)),
    StructField("discount_amount", DecimalType(10, 2)),
    StructField("ingestion_ts", TimestampType()),
    StructField("ingest_date", StringType()),
    StructField("source_file", StringType())
])

shipments_schema = StructType([
    StructField("order_id", StringType()),
    StructField("carrier", StringType()),
    StructField("shipping_cost", DecimalType(10, 2)),
    StructField("shipped_ts", TimestampType()),
    StructField("delivered_ts", TimestampType()),
    StructField("delivery_status", StringType()),
    StructField("ingestion_ts", TimestampType()),
    StructField("ingest_date", StringType()),
    StructField("source_file", StringType())
])

customers_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("state", StringType()),
    StructField("city", StringType()),
    StructField("created_ts", TimestampType()),
    StructField("ingestion_ts", TimestampType()),
    StructField("ingest_date", StringType()),
    StructField("source_file", StringType())
])

products_schema = StructType([
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("created_ts", TimestampType()),
    StructField("ingestion_ts", TimestampType()),
    StructField("ingest_date", StringType()),
    StructField("source_file", StringType())
])
