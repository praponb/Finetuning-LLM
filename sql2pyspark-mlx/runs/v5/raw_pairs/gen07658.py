from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("reading_ts") >= "2023-04-01"))
    .select("*", F.round((F.col("temp_c") * 1.15), 2).alias("temp_c_adjusted"))
)

result = (
    filtered
    .groupBy("firmware")
    .pivot("device_code", ["DV-04", "DV-03"])
    .agg(F.avg("status_bits").alias("total"))
    .select("firmware", F.col("DV-04").alias("dv_04"), F.col("DV-03").alias("dv_03"))
)
