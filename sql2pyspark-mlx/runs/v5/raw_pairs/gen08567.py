from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select("*", F.col("temp_c").cast("int").alias("temp_c_int"))
)

result = (
    cleaned
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-01"])
    .agg(F.max("temp_c").alias("value_sum"))
    .select("firmware", F.col("DV-03").alias("dv_03"), F.col("DV-01").alias("dv_01"))
)
