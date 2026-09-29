from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("log_ts") >= "2024-03-03")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "context",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.max("flag_bit4").alias("max_flag_bit4"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
    )
)
