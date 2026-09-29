from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("firmware", "id")
    .subtract(
        df_device_readings
        .filter(~F.col("firmware").isin(["1.1.0", "2.0.1"]))
        .select("firmware", "id")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 11:00:00")))
    .select(
        "*",
        F.hour("reading_ts").alias("reading_ts_hour"),
        F.concat_ws(",", "firmware", "device_code").alias("firmware_device_code_key"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "reading_ts_hour",
        "firmware_device_code_key",
        F.when(F.col("temp_c") >= 35.7, "High").when(F.col("temp_c") >= 24.6, "Medium").otherwise("Low").alias("temp_c_tier"),
    )
)

result = (
    valid_device_readings
    .select("device_code", "temp_c", "pressure", "humidity")
    .unpivot(["device_code"], ["temp_c", "pressure", "humidity"], "measure", "val")
)
