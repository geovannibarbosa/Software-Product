# ============================================
# IMPORTS
# ============================================
from pyspark.sql.functions import (col, when, trim, row_number,regexp_replace, floor)
from pyspark.sql.window import Window
from spark_session import get_spark
from schemas import customers_schema


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
spark = get_spark("silver-order-items")


# ============================================
# DEFINICAO DOS PATHS
# ============================================
bronze_path = "/opt/spark/lakehouse/bronze/order_items"
silver_path = "/opt/spark/lakehouse/silver/order_items"
csv_path = "/opt/spark/exports/silver/order_items"


# ============================================
# LEITURA DA CAMADA BRONZE
# ============================================
df = spark.read.parquet(bronze_path)


# ============================================
# VALIDACOES INICIAIS
# ============================================
df = df.filter(
    col("order_id").isNotNull()
    & col("product_id").isNotNull()
)


# ============================================
# PADRONIZACAO DE TIPOS
# ============================================
df = (
    df
    .withColumn("order_id", col("order_id").cast("string"))
    .withColumn("product_id", col("product_id").cast("string"))
    .withColumn("quantity", regexp_replace(trim(col("quantity")), ",", ".").cast("decimal(10,2)"))
    .withColumn("unit_price", regexp_replace(trim(col("unit_price")), ",", ".").cast("decimal(10,2)"))
    .withColumn("discount_amount", regexp_replace(trim(col("discount_amount")), ",", ".").cast("decimal(10,2)"))
    .withColumn("ingestion_ts", col("ingestion_ts").cast("timestamp"))
    .withColumn("ingest_date", col("ingest_date").cast("date"))
)


# ============================================
# DEDUPLICACAO
# ============================================
window = Window.partitionBy("order_id", "product_id").orderBy(col("ingestion_ts").desc())
df = (df.withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn"))


# ============================================
# NORMALIZACOES SILVER
# ============================================
df = (
    df
    # Desconto nulo vira zero
    .withColumn(
        "discount_amount",
        when(col("discount_amount").isNull(), 0.00)
        .otherwise(col("discount_amount"))
    )

    # Quantidade inteira e válida
    .withColumn("quantity", floor(col("quantity")).cast("int"))
    .withColumn(
        "quantity",
        when(col("quantity") < 0, None).otherwise(col("quantity"))
    )

    # Preços inválidos
    .withColumn(
        "unit_price",
        when(col("unit_price") < 0, None).otherwise(col("unit_price"))
    )
    .withColumn(
        "discount_amount",
        when(col("discount_amount") < 0, 0).otherwise(col("discount_amount"))
    )

    # Flags de qualidade
    .withColumn("has_invalid_quantity", col("quantity").isNull())
    .withColumn(
        "is_valid_item",
        col("quantity").isNotNull() & col("unit_price").isNotNull()
    )
)


# ============================================
# SELECAO FINAL DE COLUNAS
# ============================================
df_final = df.select(
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
    "ingestion_ts",
    "ingest_date",
    "has_invalid_quantity",
    "is_valid_item"
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
    output_path=csv_path,
)


# ============================================
# FINALIZADO
# ============================================
spark.stop()
