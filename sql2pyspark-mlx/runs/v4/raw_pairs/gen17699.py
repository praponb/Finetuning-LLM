from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

filtered = (
    flattened
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-02"))) & F.col("k").isNotNull() & F.col("service").isin(["search", "auth", "checkout"]))
    .select(
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "k",
        F.coalesce("latency_ms", F.lit(0)).alias("latency_ms_filled"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
    )
)

base = (
    filtered
    .filter(F.col("host").isin(["h3", "h2", "h4"]))
    .select(
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "latency_ms_filled",
        F.to_date("log_ts").alias("log_ts_date"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

stats = (
    base
    .groupBy(
        F.date_trunc("week", "log_ts_date").alias("week_start"),
        F.date_trunc("year", "log_ts_date").alias("year_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.sum(F.when(F.col("host") == "h4", F.col("latency_ms"))).alias("h4_total_latency_ms"),
    )
)

result = stats.select("week_start", "year_start", "duration_min", "h4_total_latency_ms")
