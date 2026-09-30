from pyspark.sql import functions as F

matched_keys = (
    df_device_readings
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") >= "2023-08-01")
        .select("device_code")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("status_bits"))).alias("dv_02_total_status_bits"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("status_bits"))).alias("dv_04_total_status_bits"),
    )
    .orderBy(F.col("dv_04_total_status_bits").desc_nulls_first())
)
