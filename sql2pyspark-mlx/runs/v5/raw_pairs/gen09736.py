from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "value"),
    )
)

base = (
    lines
    .filter(~F.col("key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "value",
        F.substring("host", 1, 3).alias("host_prefix"),
    )
)

valid = (
    base
    .filter(~F.col("level").isin(["DEBUG", "INFO", "WARN"]))
    .select(
        "id",
        "log_ts",
        "level",
        "value",
        F.concat("host", F.lit(", "), "service").alias("host_service"),
    )
)

cleaned = (
    valid
    .filter(F.col("level") != "ERROR")
    .select(
        "id",
        "log_ts",
        "value",
        "host_service",
        F.when(F.col("level").isin(["INFO", "DEBUG"]), "Tier 1").otherwise("Other").alias("level_group"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.max("log_ts").alias("ended_at"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.max("log_ts").alias("ended_at_2"),
        F.min("log_ts").alias("started_at"),
        F.min("log_ts").alias("first_seen"),
        F.min("log_ts").alias("first_log_ts"),
    )
    .orderBy(F.col("span_minutes").asc_nulls_last())
)
