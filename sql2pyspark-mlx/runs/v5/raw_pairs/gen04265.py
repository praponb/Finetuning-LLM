from pyspark.sql import functions as F

matched_keys = (
    df_device_readings
    .select("id", "device_code")
    .intersectAll(
        df_device_readings
        .filter(F.col("status_bits") == 11)
        .select("id", "device_code")
    )
)

kept_rows = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "device_code",
        F.when(F.col("humidity") >= 68, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
)
