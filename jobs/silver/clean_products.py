# ============================================
# IMPORTS
# ============================================
from pyspark.sql.functions import (col, when, upper, initcap, trim, to_timestamp, to_date, row_number, translate, lower)
from pyspark.sql.window import Window
from spark_session import get_spark
from schemas import products_schema


# ============================================
# FUNCOES AUXILIARES
# ============================================
def remove_accents(coluna):
    return translate(
        coluna,
        "áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ",
        "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"
    )


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
spark = get_spark("silver-products")


# ============================================
# DEFINICAO DOS PATHS
# ============================================
bronze_path = "/opt/spark/lakehouse/bronze/products"
silver_path = "/opt/spark/lakehouse/silver/products"
csv_path = "/opt/spark/exports/silver/products"


# ============================================
# LEITURA DA CAMADA BRONZE
# ============================================
df = spark.read.parquet(bronze_path)


# ============================================
# VALIDACOES INICIAIS
# ============================================
df = df.filter(col("product_id").isNotNull())


# ============================================
# PADRONIZACAO DE TIPOS
# ============================================
df = (
    df
    .withColumn("product_id", col("product_id").cast("string"))
    .withColumn("category", col("category").cast("string"))
    .withColumn("brand", col("brand").cast("string"))
    .withColumn("created_ts", to_timestamp("created_ts"))
    .withColumn("ingestion_ts", col("ingestion_ts").cast("timestamp"))
    .withColumn("ingest_date", col("ingest_date").cast("date"))
)


# ============================================
# DEDUPLICACAO
# ============================================
window = Window.partitionBy("product_id").orderBy(col("ingestion_ts").desc())
df = (
    df
    .withColumn("rn", row_number().over(window))
    .filter("rn = 1")
    .drop("rn")
)


# ============================================
# NORMALIZACOES SILVER
# ============================================
df = (
    df
    # CATEGORY
    .withColumn(
        "category",
        when(
            col("category").isNull() | (trim(col("category")) == ""),
            "Unknown"
        ).otherwise(
            initcap(
                lower(
                    remove_accents(trim(col("category")))
                )
            )
        )
    )

    # BRAND
    .withColumn(
        "brand",
        when(col("brand").isNull() | (trim(col("brand")) == ""), "UNKNOWN")
        .otherwise(upper(trim(col("brand"))))
    )

    # CREATED DATE
    .withColumn("product_created_date", to_date(col("created_ts")))
)


df = (
    df
    .withColumn(
        "has_null_required_fields",
        (
            when(col("product_id").isNull(), 1).otherwise(0)
            + when(col("category") == "UNKNOWN", 1).otherwise(0)
            + when(col("brand") == "UNKNOWN", 1).otherwise(0)
        ) > 0
    )
    .withColumn(
        "is_valid_record",
        col("has_null_required_fields") == False
    )
)


# ============================================
# SELECAO FINAL DE COLUNAS
# ============================================
df_final = df.select(
    "product_id",
    "category",
    "brand",
    "created_ts",
    "product_created_date",
    "ingestion_ts",
    "ingest_date",
    "has_null_required_fields",
    "is_valid_record"
)


# ============================================
# ESCRITA DA CAMADA SILVER
# ============================================
(
    df_final
    .write
    .mode("overwrite")
    .partitionBy("ingest_date")
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
