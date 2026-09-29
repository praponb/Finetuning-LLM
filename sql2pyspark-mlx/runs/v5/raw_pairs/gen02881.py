from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.col("context")["version"].alias("version"),
        F.col("context").getItem("trace").alias("trace"),
        F.size("context").alias("num_context"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    unpacked
    .rollup(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        "level",
        F.when(F.col("flag_bit4") >= 1, "LARGE").otherwise("SMALL").alias("flag_bit4_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") < 1000000)
    .orderBy("span_minutes")
)
