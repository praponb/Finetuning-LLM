from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 03:00:00").cast("timestamp"))
    .withColumn("is_dv_02", F.when(F.col("device_code") == "DV-02", 1).otherwise(0))
    .withColumn("pressure_plus_humidity", (F.col("pressure") + F.col("humidity")))
)

result = (
    base
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-04"])
    .agg(F.sum("pressure"))
    .select("firmware", F.col("DV-03").alias("dv_03"), F.col("DV-04").alias("dv_04"))
)
