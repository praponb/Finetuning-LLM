from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")

cleaned = (
    df_device_readings
    .filter(F.col("humidity") > 68)
    .withColumn("humidity_plus_status_bits", (F.col("humidity") + F.col("status_bits")))
)

cleaned_device_readings = (
    cleaned
    .filter(F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .select("*", F.col("pressure").cast("int").alias("pressure_int"))
)

cleaned_device_readings_2 = (
    cleaned_device_readings
    .filter(F.col("status_bits").between(5, 10))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "pressure_int",
        (F.col("pressure_int") - F.col("humidity")).alias("pressure_int_minus_humidity"),
    )
)

enriched = (
    cleaned_device_readings_2
    .select(
        "*",
        F.lead("status_bits").over(w).alias("next_status_bits"),
        F.lead("pressure_int").over(w).alias("next_pressure_int"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("day", "reading_ts").alias("activity_day"),
        F.when(F.col("next_pressure_int") >= 82, "LARGE").otherwise("SMALL").alias("next_pressure_int_tier"),
        "device_code",
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("status_bits")).otherwise(0)).alias("dv_04_status_bits"),
    )
)
