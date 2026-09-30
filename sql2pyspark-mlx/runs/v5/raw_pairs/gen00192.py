from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("status_bits") >= 4)
    .select("*", (F.col("temp_c") - F.col("humidity")).alias("temp_c_minus_humidity"))
)

result = (
    filtered
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-04"])
    .agg(F.avg("pressure"))
    .select("firmware", F.col("DV-01").alias("dv_01"), F.col("DV-04").alias("dv_04"))
)
