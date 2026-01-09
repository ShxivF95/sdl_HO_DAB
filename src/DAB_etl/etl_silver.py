import dlt

catalog_name = spark.conf.get("pipelines.catalog")
silver_schema = spark.conf.get("pipelines.target")

BRONZE_SCHEMA = "bronze_stg"

# ===================== acxtestdone (CDC SCD-1) =====================

@dlt.view(name="vw_br_acxtestdone")
def vw_br_acxtestdone():
    return spark.readStream.table(
        f"{catalog_name}.{BRONZE_SCHEMA}.br_acxtestdone"
    )

dlt.create_streaming_table(
    name="sil_acxtestdone",
    comment="Silver SCD-1 via auto CDC"
)

dlt.create_auto_cdc_flow(
    target="sil_acxtestdone",
    source="vw_br_acxtestdone",
    keys=["SALESID", "ITEMID"],
    sequence_by="Billed_datetime",
    stored_as_scd_type=1
)

# ===================== doctorattendance (append-only) =====================

@dlt.table(
    name="sil_doctorattendance",
    comment="Silver append-only attendance fact table"
)
def sil_doctorattendance():
    return spark.read.table(
        f"{catalog_name}.{BRONZE_SCHEMA}.br_doctorattendance"
    )

# ===================== routinetests (append-only) =====================

@dlt.table(
    name="sil_routinetests",
    comment="Silver append-only routine test facts"
)
def sil_routinetests():
    return spark.read.table(
        f"{catalog_name}.{BRONZE_SCHEMA}.br_routinetests"
    )

# ===================== patientsdata (CDC SCD-2) =====================

@dlt.view(name="vw_br_patientsdata")
def vw_br_patientsdata():
    return spark.readStream.table(
        f"{catalog_name}.{BRONZE_SCHEMA}.br_patientsdata"
    )

dlt.create_streaming_table(
    name="sil_patientsdata",
    comment="Silver SCD-2 via auto CDC"
)

dlt.create_auto_cdc_flow(
    target="sil_patientsdata",
    source="vw_br_patientsdata",
    keys=["Patient_ID"],
    sequence_by="ingested_at",
    stored_as_scd_type=2,
    track_history_column_list=[
        "Patient_Name",
        "GENDER",
        "Mobile_no",
        "ZIPCODE",
        "DOB"
    ]
)
