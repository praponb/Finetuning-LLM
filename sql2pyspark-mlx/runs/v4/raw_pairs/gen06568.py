from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-02", "DV-01"])
    .agg(F.avg("status_bits"))
    .select("firmware", F.col("DV-02").alias("dv_02"), F.col("DV-01").alias("dv_01"))
)
