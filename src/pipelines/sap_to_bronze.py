import dlt
from pyspark.sql.functions import col, current_timestamp
from config import get_config


@dlt.table(
    name="sap_sdp_ingestion",
    comment="Bronze table for SAP Ingestion, data from ADLS landing layer",
    table_properties={"quality": "bronze", "source": "sap", "project": "topsoe"},
)
def sap_sdp_ingestion():
    cfg = get_config(spark)

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{cfg.schema_location}")
        .option("cloudFiles.maxFilesPerTrigger", cfg.max_files_per_trigger)
        .load(f"{cfg.landing_path}")
    )

    df = df.withColumn("_ingestion_time", current_timestamp()).withColumn("_source_file", col("_metadata.file_path"))

    return df
