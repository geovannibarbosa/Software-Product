# ============================================
# IMPORTS
# ============================================
import os
from pyspark.sql.functions import (col, sum, to_date, when, unix_timestamp, round, coalesce, lit)
from spark_session import get_spark


# ============================================
# FUNCOES AUXILIARES
# ============================================
def export_to_csv(df, output_path, coalesce_to_one_file=True, delimiter=";", header=True):

    writer_df = df
    if coalesce_to_one_file:
        writer_df = writer_df.coalesce(1)
    (
        writer_df
        .write
        .mode("overwrite")
        .option("header", header)
        .option("delimiter", delimiter)
        .csv(output_path)
    )


# ============================================
# INICIALIZACAO DO SPARK
# ============================================
spark = get_spark("gold-fact-orders")


# ============================================
# DEFINICAO DOS PATHS
# ============================================
orders_path = "/opt/spark/lakehouse/silver/orders"
items_path = "/opt/spark/lakehouse/gold/fact_order_items"
shipments_path = "/opt/spark/lakehouse/silver/shipments"
payments_path = "/opt/spark/lakehouse/silver/payments"

gold_path = "/opt/spark/lakehouse/gold/fact_orders"
csv_path = "/opt/spark/exports/gold/fact_orders"


# ============================================
# FUNCOES AUXILIARES DE LEITURA
# ============================================
def read_if_exists(path):
    if os.path.exists(path):
        return spark.read.parquet(path)
    return None


# ============================================
# LEITURA DAS CAMADAS
# ============================================
orders = spark.read.parquet(orders_path)
items = spark.read.parquet(items_path)

shipments = read_if_exists(shipments_path)
payments = read_if_exists(payments_path)


# ============================================
# AGREGACAO DE ITENS
# ============================================
items_agg = (
    items
    .groupBy("order_id")
    .agg(
        sum(col("quantity") * col("unit_price")).alias("gross_amount"),
        sum("discount_amount").alias("discount_total")
    )
)


# ============================================
# JOIN BASE
# ============================================
df = orders.join(items_agg, "order_id", "left")


# ============================================
# JOINS OPCIONAIS
# ============================================
if payments:
    df = df.join(
        payments.select("order_id", "payment_method"),
        "order_id",
        "left"
    )
else:
    df = df.withColumn("payment_method", lit("UNKNOWN"))


if shipments:
    df = df.join(
        shipments.select(
            "order_id",
            "carrier",
            "shipping_cost",
            "shipped_ts",
            "delivered_ts"
        ),
        "order_id",
        "left"
    )
else:
    df = (
        df
        .withColumn("carrier", lit(None))
        .withColumn("shipping_cost", lit(0))
        .withColumn("shipped_ts", lit(None))
        .withColumn("delivered_ts", lit(None))
    )


# ============================================
# TEMPO DE ENTREGA
# ============================================
df = df.withColumn(
    "delivery_time_hours",
    when(
        col("shipped_ts").isNotNull() &
        col("delivered_ts").isNotNull(),
        (
            unix_timestamp(col("delivered_ts")) -
            unix_timestamp(col("shipped_ts"))
        ) / 3600
    )
)


# ============================================
# METRICAS FINANCEIRAS
# ============================================
df = (
    df
    .withColumn("gross_amount", round(col("gross_amount"), 2))
    .withColumn("discount_total", round(col("discount_total"), 2))
    .withColumn(
        "net_amount",
        round(
            coalesce(col("gross_amount"), lit(0))
            - coalesce(col("discount_total"), lit(0))
            + coalesce(col("shipping_cost"), lit(0)),
            2
        )
    )
)


# ============================================
# STATUS FINAL
# ============================================
df = df.withColumn(
    "status_final",
    when(col("delivered_ts").isNotNull(), "DELIVERED")
    .when(col("shipped_ts").isNotNull(), "SHIPPED")
    .otherwise("CREATED")
)


# ============================================
# SLA
# ============================================
df = df.withColumn(
    "is_late",
    col("delivery_time_hours") > 72
)


# ============================================
# SELECAO FINAL DE COLUNAS
# ============================================
df_final = df.select(
    "order_id",
    "customer_id",
    to_date("order_ts").alias("order_date"),
    "order_ts",
    "gross_amount",
    "discount_total",
    "net_amount",
    "payment_method",
    "status_final",
    "carrier",
    "shipping_cost",
    "shipped_ts",
    "delivered_ts",
    "delivery_time_hours",
    "is_late"
)


# ============================================
# ESCRITA DA CAMADA GOLD
# ============================================
(
    df_final
    .write
    .mode("overwrite")
    .parquet(gold_path)
)


# ============================================
# EXPORTA CSV PARA ENTREGA FINAL
# ============================================
export_to_csv(
    df=df_final,
    output_path=csv_path
)


# ============================================
# FINALIZADO
# ============================================
spark.stop()
