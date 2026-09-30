from pyspark.sql import functions as F

matched_keys = (
    df_device_readings
    .select("firmware", "id")
    .intersect(
        df_device_readings
        .filter(F.col("pressure").between(1010, 1021))
        .select("firmware", "id")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("status_bits") >= 10, "PREMIUM").otherwise("STANDARD").alias("status_bits_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max("reading_ts").alias("last_seen"),
        F.corr("humidity", "temp_c").alias("corr_humidity_temp_c"),
    )
    .orderBy("num_rows")
)
