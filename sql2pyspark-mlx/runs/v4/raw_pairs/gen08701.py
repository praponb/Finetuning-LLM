from pyspark.sql import functions as F

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "humidity",
        F.col("temp_c").alias("TEMP_C"),
        "pressure",
    )
    .unpivot(
        ["id", "device_code"],
        ["humidity", "TEMP_C", "pressure"],
        "metric_name",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
