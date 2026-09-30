from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.concat("firmware", F.lit(" - "), "device_code").alias("firmware_device_code"),
    )
)

result = (
    filtered
    .cube(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("temp_c") >= 41.3, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("status_bits"))).alias("dv_02_total_status_bits"),
        F.percentile_approx("pressure", 0.5).alias("pressure_p50"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("humidity")).otherwise(0)).alias("dv_01_humidity"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("reading_ts").alias("ended_at"),
    )
    .filter(F.col("dv_01_humidity") >= 0)
)
