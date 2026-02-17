from dataclasses import dataclass


@dataclass
class SapSDPIngestionConfig:
    catalog: str
    schema_bronze: str
    landing_path: str
    checkpoint_path: str
    schema_location: str
    max_files_per_trigger: int


def get_config(spark):
    catalog = spark.conf.get("sap_sdp_ingestion.catalog")
    schema_bronze = spark.conf.get("sap_sdp_ingestion.schema.bronze")
    landing_path = spark.conf.get("sap_sdp_ingestion.adls.landing.path")
    checkpoint_path = spark.conf.get("sap_sdp_ingestion.checkpoint.path")
    schema_location = spark.conf.get("sap_sdp_ingestion.schema_path")
    max_files_per_trigger = spark.conf.get("sap_sdp_ingestion.max_files_per_trigger")

    return SapSDPIngestionConfig(
        catalog=catalog,
        schema_bronze=schema_bronze,
        landing_path=landing_path,
        checkpoint_path=checkpoint_path,
        schema_location=schema_location,
        max_files_per_trigger=max_files_per_trigger,
    )
