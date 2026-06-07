# ============================================
# IMPORTS
# ============================================
from pyspark.sql.functions import (
    col, when, trim, upper, lower,
    to_timestamp, regexp_replace,
    isnan, row_number, initcap
)
from pyspark.sql.window import Window
from spark_session import get_spark
from schemas import shipments_schema


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
spark = get_spark("silver-shipments")


# ============================================
# DEFINICAO DOS PATHS
# ============================================
bronze_path = "/opt/spark/lakehouse/bronze/shipments"
silver_path = "/opt/spark/lakehouse/silver/shipments"
csv_path = "/opt/spark/exports/silver/shipments"


# ============================================
# LEITURA DA CAMADA BRONZE
# ============================================
df = spark.read.parquet(bronze_path)


# ============================================
# VALIDACOES INICIAIS
# ============================================
df = df.filter(col("order_id").isNotNull())


# ============================================
# PADRONIZACAO DE TIPOS
# ============================================
df = (
    df
    .withColumn(
        "shipped_ts",
        when(col("shipped_ts") == "N/A", None)
        .otherwise(col("shipped_ts"))
    )
    .withColumn(
        "delivered_ts",
        when(col("delivered_ts") == "N/A", None)
        .otherwise(col("delivered_ts"))
    )
    .withColumn("shipped_ts", to_timestamp("shipped_ts"))
    .withColumn("delivered_ts", to_timestamp("delivered_ts"))
    .withColumn("shipping_cost", regexp_replace(trim(col("shipping_cost")), ",", "."))
    .withColumn("shipping_cost", col("shipping_cost").cast("decimal(10,2)"))
    .withColumn("ingestion_ts", col("ingestion_ts").cast("timestamp"))
    .withColumn("ingest_date", col("ingest_date").cast("date"))
)


# ============================================
# DEDUPLICACAO
# ============================================
window = Window.partitionBy("order_id").orderBy(col("ingestion_ts").desc())
df = (
    df
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)


# ============================================
# NORMALIZACOES SILVER
# ============================================
df = (
    df
    # Carrier
    .withColumn(
        "carrier",
        when(col("carrier").isNull(), "Unknown")
        .otherwise(initcap(lower(trim(col("carrier")))))
    )

    # Delivery status
    .withColumn(
        "delivery_status",
        when(col("delivery_status").isNull(), "unknown")
        .otherwise(lower(trim(col("delivery_status"))))
    )

    # Flag default
    .withColumn(
        "is_shipping_cost_defaulted",
        col("shipping_cost").isNull() | isnan(col("shipping_cost"))
    )

    # Default zero
    .withColumn(
        "shipping_cost",
        when(col("shipping_cost").isNull(), 0.00)
        .otherwise(col("shipping_cost"))
    )

    # Campos obrigatórios
    .withColumn(
        "has_null_required_fields",
        col("order_id").isNull() |
        col("delivery_status").isNull()
    )

    # Valores inválidos
    .withColumn(
        "has_invalid_values",
        col("shipping_cost") < 0
    )

    # Registro válido
    .withColumn(
        "is_valid_record",
        ~(col("has_null_required_fields") | col("has_invalid_values"))
    )
)


# ============================================
# SELECAO FINAL DE COLUNAS
# ============================================
df_final = df.select(
    "order_id",
    "carrier",
    "shipping_cost",
    "shipped_ts",
    "delivered_ts",
    "delivery_status",
    "ingestion_ts",
    "ingest_date",
    "is_shipping_cost_defaulted",
    "has_null_required_fields",
    "has_invalid_values",
    "is_valid_record"
)


# ============================================
# ESCRITA DA CAMADA SILVER
# ============================================
(
    df_final
    .write
    .mode("overwrite")
    .parquet(silver_path)
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
