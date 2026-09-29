from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("device_code")
    .intersectAll(
        df_device_readings
        .filter(F.hour("reading_ts").between(10, 18))
        .select("device_code")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("status_bits") >= 12, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
    )
    .agg(
        F.corr("temp_c", "humidity").alias("corr_temp_c_humidity"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("cnt") >= 1)
    .orderBy(F.col("status_bits_tier").desc())
)
