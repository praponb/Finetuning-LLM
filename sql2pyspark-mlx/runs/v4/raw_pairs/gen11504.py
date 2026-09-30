from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("temp_c") <= 33))
    .withColumn("device_code_upper", F.upper("device_code"))
    .withColumn("humidity_plus_status_bits", (F.col("humidity") + F.col("status_bits")))
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        F.col("temp_c").alias("TEMP_C_VAL"),
        "humidity",
        F.col("humidity_plus_status_bits").alias("HUMIDITY_PLUS_STATUS_BITS_VAL"),
    )
    .unpivot(
        ["id", "device_code"],
        ["TEMP_C_VAL", "humidity", "HUMIDITY_PLUS_STATUS_BITS_VAL"],
        "metric_name",
        "metric_value",
    )
)
