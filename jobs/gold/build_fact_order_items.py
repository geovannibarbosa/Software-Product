# ============================================
# IMPORTS
# ============================================
from pyspark.sql.functions import (col, when, round)
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
spark = get_spark("gold-fact-order-items")


# ============================================
# DEFINICAO DOS PATHS
# ============================================
silver_path = "/opt/spark/lakehouse/silver/order_items"
gold_path = "/opt/spark/lakehouse/gold/fact_order_items"
csv_path = "/opt/spark/exports/gold/fact_order_items"


# ============================================
# LEITURA DA CAMADA SILVER
# ============================================
df = spark.read.parquet(silver_path)


# ============================================
# VALIDACOES INICIAIS / REGRAS DE NEGOCIO
# ============================================
df = df.filter(
    (col("order_id").isNotNull()) &
    (col("product_id").isNotNull()) &
    (col("quantity") > 0) &
    (col("unit_price") >= 0)
)


# ============================================
# PADRONIZACAO DE VALORES
# ============================================
df = df.withColumn(
    "discount_amount",
    when(col("discount_amount") < 0, 0)
    .otherwise(col("discount_amount"))
)


# ============================================
# CALCULO DE VALOR LIQUIDO
# ============================================
df = df.withColumn(
    "item_net_amount",
    round(
        (col("quantity") * col("unit_price")) - col("discount_amount"),
        2
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
    "item_net_amount"
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
