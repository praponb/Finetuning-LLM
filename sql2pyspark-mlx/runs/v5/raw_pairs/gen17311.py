from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select("*", F.round("temp_c", 2).alias("temp_c_rounded"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("pressure") >= 1029, "HIGH").otherwise("LOW").alias("pressure_tier"),
        F.when(F.col("humidity") >= 73, "PREMIUM").otherwise("STANDARD").alias("humidity_tier"),
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("temp_c_rounded")).otherwise(0)).alias("dv_01_temp_c_rounded"),
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("pressure_stddev")
)
