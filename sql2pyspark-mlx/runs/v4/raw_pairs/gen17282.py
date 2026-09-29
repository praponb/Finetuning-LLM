from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.to_date("reading_ts").alias("reading_ts_date"),
        (F.col("pressure") - F.col("temp_c")).alias("pressure_minus_temp_c"),
    )
)

result = (
    valid
    .select("device_code", "firmware")
    .exceptAll(
        df_device_readings
        .filter(F.col("status_bits") < 3)
        .select("device_code", "firmware")
    )
)
