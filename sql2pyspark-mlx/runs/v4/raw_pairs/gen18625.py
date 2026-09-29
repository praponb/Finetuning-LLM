from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-04", "DV-02"])
    .agg(F.avg("humidity").alias("value_sum"))
    .select(
        "firmware",
        F.col("DV-01").alias("dv_01"),
        F.col("DV-04").alias("dv_04"),
        F.col("DV-02").alias("dv_02"),
    )
)
