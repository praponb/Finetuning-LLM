from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("humidity") >= 58)
    .select(
        "*",
        F.col("temp_c").cast("int").alias("temp_c_int"),
        F.trim("firmware").alias("clean_firmware"),
    )
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        F.col("pressure").alias("PRESSURE_VAL"),
        F.col("humidity").alias("HUMIDITY"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["PRESSURE_VAL", "HUMIDITY"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
