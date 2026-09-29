from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("retry_count") >= 4, "PREMIUM").otherwise("STANDARD").alias("retry_count_tier"),
        "service",
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.max(F.when(F.col("service") == "checkout", F.col("flag_bit1"))).alias("checkout_max_flag_bit1"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes_2"),
        F.max("log_ts").alias("last_log_ts"),
    )
    .filter(F.col("span_minutes") > 0)
    .orderBy(F.col("checkout_max_flag_bit1").asc_nulls_first())
)
