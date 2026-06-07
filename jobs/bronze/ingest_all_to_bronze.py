# ============================================
# IMPORTS
# ============================================
import os
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit
)
from spark_session import get_spark


# ============================================
# CONSTANTES
# ============================================
BASE_INPUT_PATH = "/opt/spark/data_input"
BASE_OUTPUT_PATH = "/opt/spark/lakehouse/bronze"
CSV_OUTPUT_PATH = "/opt/spark/exports/bronze"


# ============================================
# FUNCOES AUXILIARES
# ============================================
def get_ingest_dates():
    return [
        f.replace("ingest_date=", "")
        for f in os.listdir(BASE_INPUT_PATH)
        if f.startswith("ingest_date=")
    ]


def get_entities(ingest_date):
    path = f"{BASE_INPUT_PATH}/ingest_date={ingest_date}"
    return [
        f.replace(".csv", "")
        for f in os.listdir(path)
        if f.endswith(".csv")
    ]


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
# FUNCAO PRINCIPAL
# ============================================
def main():

    # ============================================
    # INICIALIZACAO DO SPARK
    # ============================================
    spark = get_spark("bronze-full-ingestion")

    ingest_dates = get_ingest_dates()

    for ingest_date in ingest_dates:

        entities = get_entities(ingest_date)

        for entity in entities:

            input_path = (
                f"{BASE_INPUT_PATH}/ingest_date={ingest_date}/{entity}.csv"
            )

            bronze_output_path = f"{BASE_OUTPUT_PATH}/{entity}"
            csv_output_path = (
                f"{CSV_OUTPUT_PATH}/{entity}/ingest_date={ingest_date}"
            )

            print(f"Ingesting {entity} - {ingest_date}")

            # ============================================
            # LEITURA BRUTA + METADADOS
            # ============================================
            df_final = (
                spark.read
                .option("header", True)
                .csv(input_path)
                .withColumn("ingestion_ts", current_timestamp())
                .withColumn("ingest_date", lit(ingest_date))
                .withColumn("source_file", input_file_name())
            )

            # ============================================
            # ESCRITA PARQUET (BRONZE)
            # ============================================
            (
                df_final
                .write
                .mode("append")
                .partitionBy("ingest_date")
                .parquet(bronze_output_path)
            )

            # ============================================
            # EXPORTA CSV BRUTO (BRONZE)
            # ============================================
            export_to_csv(
                df=df_final,
                output_path=csv_output_path
            )

    # ============================================
    # FINALIZADO
    # ============================================
    spark.stop()


# ============================================
# ENTRYPOINT
# ============================================
if __name__ == "__main__":
    main()
