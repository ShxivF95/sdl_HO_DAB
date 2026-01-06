import dlt
from pyspark.sql.functions import col, current_timestamp

adls_path = "abfss://suraksha@hisadls.dfs.core.windows.net"

source_tables = {
    "br_acxtestdone": "acxtestdone",
    "br_doctorattendance": "doctorattendance",
    "br_inventorymaster": "inventorymaster",
    "br_patientsdata": "patientsdata",
    "br_routinetests": "routinetests"
}

def create_bronze_table(table_name, folder_path):
    @dlt.table(
        name=table_name,
        comment=f"Raw ingestion from ADLS folder: {folder_path}"
    )
    def streaming_table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load(f"{adls_path}/{folder_path}")
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("ingested_at", current_timestamp())
        )

# Loop and register the tables
for target_table, source_folder in source_tables.items():
    create_bronze_table(target_table, source_folder)