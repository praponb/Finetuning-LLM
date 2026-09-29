from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 10:00:00"))) & (F.col("temp_c") >= 37))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.greatest("pressure", "temp_c").alias("greatest_pressure_temp_c"),
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

result = (
    valid
    .groupBy("device_code")
    .agg(
        F.percentile_approx("humidity", 0.9).alias("humidity_p90"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("greatest_pressure_temp_c")).otherwise(0)).alias("dv_02_greatest_pressure_temp_c"),
    )
)
