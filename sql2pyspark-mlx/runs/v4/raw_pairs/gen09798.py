from pyspark.sql import functions as F

remaining_keys = (
    df_device_readings
    .select("id", "firmware")
    .subtract(
        df_device_readings
        .filter(F.to_date("reading_ts") >= "2023-07-15")
        .select("id", "firmware")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "device_code",
        F.when(F.col("status_bits") >= 11, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("status_bits", 0.5).alias("status_bits_p50"),
    )
    .filter(F.col("cnt") < 1)
    .orderBy(F.col("cnt").desc())
)
