# ============================================
# IMPORTS
# ============================================
from pyspark.sql.functions import (col, lower, upper, when, to_timestamp, trim, row_number, initcap, translate)
from pyspark.sql.window import Window
from spark_session import get_spark
from schemas import customers_schema


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
spark = get_spark("silver-customers")


# ============================================
# DEFINICAO DOS PATHS
# ============================================
bronze_path = "/opt/spark/lakehouse/bronze/customers"
silver_path = "/opt/spark/lakehouse/silver/customers"
csv_path = "/opt/spark/exports/silver/customers"


# ============================================
# LEITURA DA CAMADA BRONZE
# ============================================
df = spark.read.parquet(bronze_path)


# ============================================
# VALIDACOES INICIAIS
# ============================================

# Remove registros sem chave primaria
df = df.filter(col("customer_id").isNotNull())


# ============================================
# PADRONIZACAO DE TIPOS
# ============================================
df = (
    df
    .withColumn("customer_id", col("customer_id").cast("string"))
    .withColumn("state", upper(trim(col("state"))).cast("string"))
    .withColumn("city", initcap(lower(remove_accents(trim(col("city"))))).cast("string"))
    .withColumn("created_ts", to_timestamp("created_ts"))
    .withColumn("ingestion_ts", col("ingestion_ts").cast("timestamp"))
    .withColumn("source_file", col("source_file").cast("string"))
    .withColumn("ingest_date", col("ingest_date").cast("date"))
)


# ============================================
# DEDUPLICACAO
# ============================================
window = Window.partitionBy("customer_id").orderBy(col("ingestion_ts").desc())
df = (df.withColumn("rn", row_number()
        .over(window))
        .filter("rn = 1")
        .drop("rn"))


# ============================================
# NORMALIZACOES SILVER
# ============================================
df = (
    df
    # Flag de campos obrigatorios nulos
    .withColumn("has_null_required_fields",
        when(col("customer_id").isNull() 
            |col("state").isNull() 
            &col("city").isNull(), 
            True)
        .otherwise(False)
    )

    # Padroniza UF
    .withColumn("state",
        when(col("state").isNull() 
            |(trim(col("state")) == ""),
            "NA")
        .otherwise(col("state"))
    )

   # Padroniza Cidade
    .withColumn("city",
        when(col("city").isNull() 
            |(trim(col("city")) == ""), 
            "Unknown")
        .otherwise(col("city"))
    )
)


# ============================================
# SELECAO FINAL DE COLUNAS
# ============================================
df_final = df.select(
        "customer_id",
        "state",
        "city",
        "created_ts",
        "ingestion_ts",
        "ingest_date",
        "has_null_required_fields"
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
