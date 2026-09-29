from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("humidity").between(71, 74))
    .withColumn("temp_c_int", F.col("temp_c").cast("int"))
)

result = (
    filtered
    .select("id", "device_code", "humidity", "temp_c", "pressure")
    .unpivot(
        ids=["id", "device_code"],
        values=["humidity", "temp_c", "pressure"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
