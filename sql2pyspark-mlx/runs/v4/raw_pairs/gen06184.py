from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 16:00:00").cast("timestamp")) & (F.to_date("reading_ts") >= "2023-10-01"))
    .select(
        "*",
        F.lower("device_code").alias("device_code_lower"),
        F.trim("firmware").alias("clean_firmware"),
    )
)

result = (
    cleaned
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-02", "DV-03"])
    .agg(F.max("temp_c").alias("total"))
    .select(
        "firmware",
        F.col("DV-01").alias("dv_01"),
        F.col("DV-02").alias("dv_02"),
        F.col("DV-03").alias("dv_03"),
    )
)
