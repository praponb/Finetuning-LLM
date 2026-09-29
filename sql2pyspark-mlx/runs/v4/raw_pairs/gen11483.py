from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "service",
        "log_ts",
        "latency_ms",
        "attr_key",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    parsed
    .filter((F.col("log_ts") >= F.lit("2025-06-01 06:00:00").cast("timestamp")) & F.col("latency_ms").between(1534, 1710))
    .select(
        "service",
        "log_ts",
        "latency_ms",
        "attr_key",
        "flag_bit1",
        F.when(F.col("service").isin(["auth", "billing"]), "Group A").otherwise("Other").alias("service_group"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("resolved_attr_key"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.avg("flag_bit1").alias("avg_flag_bit1"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
    .orderBy(F.col("hour_window").desc())
)
