from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("device_code").isin(["DV-01", "DV-02"]))
    .withColumn("is_1_0_3", F.when(F.col("firmware") == "1.0.3", 1).otherwise(0))
)

result = (
    filtered
    .select("id", "device_code", "temp_c", "humidity", "pressure")
    .unpivot(
        ["id", "device_code"],
        ["temp_c", "humidity", "pressure"],
        "metric_name",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
