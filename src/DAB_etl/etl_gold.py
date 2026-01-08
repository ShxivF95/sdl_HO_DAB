import dlt
from pyspark.sql.functions import expr

# DLT-provided pipeline values (WRITE location)
catalog_name = spark.conf.get("pipelines.catalog")
gold_schema  = spark.conf.get("pipelines.target")

# READ location (silver)
SILVER_SCHEMA = "silver_stg"

# ===================== patients dimension =====================

@dlt.table(
    name="gold_dim_patients",
    comment="Current patients dimension for BI"
)
def gold_dim_patients():
    return (
        spark.read.table(
            f"{catalog_name}.{SILVER_SCHEMA}.sil_patientsdata"
        )
        .filter("__END_AT IS NULL")
        .select(
            "Patient_ID",
            "Patient_Name",
            "GENDER",
            "DOB",
            "ZIPCODE",
            "Mobile_no"
        )
    )

# ===================== routine tests fact =====================

@dlt.table(
    name="gold_routinetests",
    comment="Gold routine tests with report delay metrics"
)
def gold_routinetests():
    return (
        spark.read.table(
            f"{catalog_name}.{SILVER_SCHEMA}.sil_routinetests"
        )
        .withColumn(
            "report_delay_minutes",
            expr(
                "timestampdiff(MINUTE, Test_Done_datetime, Report_released_datetime)"
            )
        )
        .withColumn(
            "report_delay_hours",
            expr(
                "timestampdiff(SECOND, Test_Done_datetime, Report_released_datetime) / 3600.0"
            )
        )
    )
