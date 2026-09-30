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
        F.explode("context").alias("key", "attr_value"),
    )
)

valid = (
    lines
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .select(
        "*",
        F.upper("service").alias("service_upper"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

remaining_keys = (
    valid
    .select("id", "service")
    .subtract(
        df_app_logs
        .filter(F.hour("log_ts").between(8, 16))
        .select("id", "service")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

pivoted = (
    verified
    .groupBy("host", "service")
    .pivot("level", ["WARN", "ERROR"])
    .agg(F.sum("latency_ms"))
    .select(
        "host",
        "service",
        F.col("WARN").alias("warn"),
        F.col("ERROR").alias("error"),
    )
)

result = (
    pivoted
    .select("host", "service")
    .orderBy(F.col("host").asc_nulls_last())
)
