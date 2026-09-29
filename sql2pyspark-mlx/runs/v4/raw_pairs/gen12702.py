from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(F.to_date("reading_ts") < "2024-10-01")
        .select("device_code")
    )
)

kept_rows = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "device_code", "reading_ts", "status_bits", "firmware")
    .orderBy(F.col("device_code").desc(), "reading_ts")
)
