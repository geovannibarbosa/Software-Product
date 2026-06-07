# ============================================
# IMPORTS
# ============================================
from pyspark.sql.functions import (col, lower, upper, when, to_timestamp, trim, regexp_replace, isnan, row_number, to_date)
from pyspark.sql.window import Window
from spark_session import get_spark
from schemas import orders_schema


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
spark = get_spark("silver-orders")


# ============================================
# DEFINICAO DOS PATHS
# ============================================
bronze_path = "/opt/spark/lakehouse/bronze/orders"
silver_path = "/opt/spark/lakehouse/silver/orders"
csv_path = "/opt/spark/exports/silver/orders"


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
    .withColumn("order_id", col("order_id").cast("string"))
    .withColumn("customer_id", col("customer_id").cast("string"))
    .withColumn("status", col("status").cast("string"))
    .withColumn("payment_method", col("payment_method").cast("string"))
    .withColumn("currency", col("currency").cast("string"))
    .withColumn("order_ts", to_timestamp("order_ts"))
    .withColumn("total_amount", regexp_replace(trim(col("total_amount")), ",", ".").cast("decimal(10,2)"))
    .withColumn("ingestion_ts", col("ingestion_ts").cast("timestamp"))
    .withColumn("ingest_date", col("ingest_date").cast("date"))
)


# ============================================
# DEDUPLICACAO
# ============================================
window = Window.partitionBy("order_id").orderBy(col("ingestion_ts").desc())
df = (df.withColumn("rn", row_number().over(window))
      .filter("rn = 1")
      .drop("rn")
)


# ============================================
# NORMALIZACOES SILVER
# ============================================
df = (
    df
    # Status nulo vira UNKNOWN
    .withColumn(
        "status",
        when(col("status").isNull(), "UNKNOWN")
        .otherwise(lower(col("status")))
    )

    # Flag de moeda não reconhecida
    .withColumn(
        "is_currency_defaulted",
        col("currency").isNull()
    )

    # Metodo de pagamento padronizado
    .withColumn(
        "payment_method",
        when(col("payment_method").isNull(), "UNKNOWN")
        .otherwise(upper(col("payment_method")))
    )

    # Fallback de timestamp
    .withColumn(
        "order_ts",
        when(col("order_ts").isNull(), col("ingest_date"))
        .otherwise(col("order_ts"))
    )

    .withColumn("order_date", to_date(col("order_ts")))

    # Flags de qualidade
    .withColumn(
        "is_total_amount_defaulted",
        col("total_amount").isNull() | isnan(col("total_amount"))
    )
    .withColumn(
        "has_foreign_currency",
        col("currency") != "BRL"
    )
    .withColumn(
        "has_null_required_fields",
        when(
            col("order_id").isNull()
            | col("customer_id").isNull()
            | col("order_ts").isNull()
            | col("total_amount").isNull(),
            True
        ).otherwise(False)
    )
    .withColumn(
        "has_invalid_values",
        col("total_amount").isNull() | (col("total_amount") < 0)
    )
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
    "customer_id",
    "order_date",
    "order_ts",
    "status",
    "payment_method",
    "total_amount",
    "currency",
    "ingestion_ts",
    "ingest_date",
    "is_currency_defaulted",
    "is_total_amount_defaulted",
    "has_foreign_currency",
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
