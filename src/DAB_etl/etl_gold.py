import dlt
from pyspark.sql.functions import *

catalog_name = spark.conf.get("pipelines.catalog")
schema_name = spark.conf.get("pipelines.silver_schema")


@dlt.table(
    name= "gold_dim_patients",
    comment= "current patients dimention for BI"
)

def gold_dim_patients():
    df = spark.readStream.table(f"{catalog_name}.{schema_name}.sil_patientsdata")\
        .filter("__END_AT is NULL")\
            .select("Patient_ID",
               "Patient_Name",
               "GENDER",
               "DOB",
               "ZIPCODE",
               "Mobile_no")
    return df

#================================================================================================

@dlt.table(
    name= "gold_routinetests"
)

def gold_routinetests():
    return (
        spark.readStream.table(f"{catalog_name}.{schema_name}.sil_routinetests")
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
   
