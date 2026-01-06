import dlt

# ===================== acxtestdone (CDC SCD-1) ================================================

# Create an empty streaming table for silver layer

dlt.create_streaming_table(
    name="sil_acxtestdone",
    comment="Silver SCD-1 via auto CDC"
)

dlt.create_auto_cdc_flow(
    target="sil_acxtestdone",
    source="LIVE.br_acxtestdone",
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
    return dlt.read_stream("LIVE.br_doctorattendance")

# ===================== routinetests (append-only) ===============================================

@dlt.table(
    name="sil_routinetests",
    comment="Silver append-only routine test facts"
)
def sil_routinetests():
    return dlt.read_stream("LIVE.br_routinetests")

# ===================== patientsdatadone (CDC SCD-2) ================================================

dlt.create_streaming_table(
    name= "sil_patientsdata",
    comment= "Silver SCD-2 via auto CDC"
)

dlt.create_auto_cdc_flow(
    target="sil_patientsdata",
    source="LIVE.br_patientsdata",
    keys=["Patient_ID"],
    sequence_by="ingested_at",
    stored_as_scd_type=2,
    track_history_column_list = ["Patient_Name", "GENDER", "Mobile_no", "ZIPCODE", "DOB"]    # demographic changes should be historized to avoiding noise
)
