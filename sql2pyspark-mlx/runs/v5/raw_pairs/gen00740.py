from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= "2023-12-04")
    .select(
        "*",
        F.trim("device_code").alias("clean_device_code"),
        F.to_date("reading_ts").alias("reading_ts_date"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("temp_c") >= 38.8, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("temp_c")).otherwise(0)).alias("dv_01_temp_c"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
    )
)
