# ============================================
# IMPORTS
# ============================================
from pyspark.sql.functions import (col, row_number, to_timestamp, coalesce)
from pyspark.sql.window import Window
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
spark = get_spark("gold-dim-customers")


# ============================================
# DEFINICAO DOS PATHS
# ============================================
silver_path = "/opt/spark/lakehouse/silver/customers"
gold_path = "/opt/spark/lakehouse/gold/dim_customers"
csv_path = "/opt/spark/exports/gold/dim_customers"


# ============================================
# LEITURA DA CAMADA SILVER
# ============================================
df = spark.read.parquet(silver_path)


# ============================================
# VALIDACOES INICIAIS
# ============================================
df = df.filter(col("customer_id").isNotNull())


# ============================================
# PADRONIZACAO DE TIPOS
# ============================================
df = (
    df
    .withColumn(
        "created_ts",
        coalesce(
            to_timestamp(col("created_ts")),
            to_timestamp(col("ingestion_ts"))
        )
    )
)


# ============================================
# DEDUPLICACAO
# ============================================
window = Window.partitionBy("customer_id").orderBy(col("created_ts").desc())
df = (df.withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)


# ============================================
# SELECAO FINAL DE COLUNAS
# ============================================
df_final = df.select(
    "customer_id",
    "state",
    "city",
    "created_ts"
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
