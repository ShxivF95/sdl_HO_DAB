import dlt

# ===================== acxtestdone (CDC SCD-1) ================================================

@dlt.view(
    name= "vw_br_acxtestdone"
)

def vw_br_acxtestdone():
    return spark.readStream.table("surakshadb.bronze_stg.br_acxtestdone")

# Create an empty streaming table for silver layer

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

# ===================== doctorattendance (append-only) ==========================================

@dlt.table(
    name="sil_doctorattendance",
    comment="Silver append-only attendance fact table"
)
def sil_doctorattendance():
    return spark.readStream.table("surakshadb.bronze_stg.br_doctorattendance")

# ===================== routinetests (append-only) ===============================================

@dlt.table(
    name="sil_routinetests",
    comment="Silver append-only routine test facts"
)
def sil_routinetests():
    return spark.readStream.table("surakshadb.bronze_stg.br_routinetests")

# ===================== patientsdatadone (CDC SCD-2) ================================================

@dlt.view(
    name= "vw_br_patientsdata"
)

def vw_br_patientsdata():
    return spark.readStream.table("surakshadb.bronze_stg.br_patientsdata")


# create an empty streaming table for silver layer

dlt.create_streaming_table(
    name= "sil_patientsdata",
    comment= "Silver SCD-2 via auto CDC"
)

dlt.create_auto_cdc_flow(
    target="sil_patientsdata",
    source="vw_br_patientsdata",
    keys=["Patient_ID"],
    sequence_by="ingested_at",
    stored_as_scd_type=2,
    track_history_column_list = ["Patient_Name", "GENDER", "Mobile_no", "ZIPCODE", "DOB"]    # demographic changes should be historized to avoiding noise
)
