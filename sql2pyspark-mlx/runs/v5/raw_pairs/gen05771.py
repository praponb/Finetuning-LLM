from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "log_ts", "status_code", F.explode_outer("context").alias("key", "v"))
)

unpacked = (
    flattened
    .select(
        "id",
        "log_ts",
        "key",
        "v",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    unpacked
    .filter(F.col("key").isNotNull())
    .select("*", F.round("has_fault", 1).alias("has_fault_rounded"))
)

valid_app_logs = (
    valid
    .filter(F.col("log_ts") > F.lit("2025-06-01 09:00:00").cast("timestamp"))
    .select(
        "id",
        "v",
        "has_fault",
        "status_code_high",
        F.date_trunc("year", "log_ts").alias("log_ts_period"),
    )
)

kept_keys = (
    valid_app_logs
    .select("id")
    .exceptAll(
        df_app_logs
        .filter(F.col("latency_ms") > 1005)
        .select("id")
    )
)

verified = (
    valid_app_logs
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("status_code_high") >= 3, "HIGH").otherwise("LOW").alias("status_code_high_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        ((F.unix_timestamp(F.max("log_ts_period")) - F.unix_timestamp(F.min("log_ts_period"))) / 60).alias("duration_min"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids_2").asc_nulls_last())
)
