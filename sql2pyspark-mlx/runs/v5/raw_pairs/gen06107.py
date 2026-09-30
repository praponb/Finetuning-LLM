from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

filtered = (
    exploded
    .filter(~F.col("level").isin(["INFO", "ERROR"]))
    .select(
        "host",
        "k",
        "value",
        F.round((F.col("status_code") / F.col("latency_ms")), 2).alias("status_code_per_latency_ms"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

valid = (
    filtered
    .filter(F.col("k").isin(["region", "trace"]))
    .withColumn("host_k_key", F.concat_ws("-", "host", "k"))
    .withColumn("clean_host", F.trim("host"))
    .withColumn("resolved_k_2", F.coalesce("k", F.lit("OTHER")))
)

kept_keys = (
    valid
    .select("host")
    .intersectAll(
        df_app_logs
        .filter(F.col("log_ts") >= "2023-04-05")
        .select("host")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("status_code_per_latency_ms") >= 78, "LARGE").otherwise("SMALL").alias("status_code_per_latency_ms_tier"),
    )
    .agg(
        F.max(F.when(F.col("host") == "h4", F.col("status_code_per_latency_ms"))).alias("h4_max_status_code_per_latency_ms"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_count"),
        F.max(F.when(F.col("k") == "trace", F.col("status_code_per_latency_ms"))).alias("trace_max_status_code_per_latency_ms"),
        F.sum(F.when(F.col("host") == "h1", F.col("status_code_per_latency_ms")).otherwise(0)).alias("h1_status_code_per_latency_ms"),
    )
    .filter(F.col("h2_count") >= 1)
    .orderBy(F.col("status_code_per_latency_ms_tier").asc_nulls_last())
)
