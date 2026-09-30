from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

remaining_keys = (
    exploded
    .select("id")
    .exceptAll(
        df_app_logs
        .filter(F.col("level").isin(["DEBUG", "WARN"]))
        .select("id")
    )
)

verified = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("trace"))))
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "v",
        F.when(F.col("attr_key") == "trace", 1).otherwise(0).alias("is_trace"),
    )
)

filtered = (
    base
    .filter(F.col("is_trace") >= 52)
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "v",
        F.upper("attr_key").alias("attr_key_upper"),
    )
)

result = (
    filtered
    .groupBy("attr_key")
    .agg(
        F.avg(F.when(F.col("attr_key") == "version", F.col("latency_ms"))).alias("version_avg_latency_ms"),
        F.count_if(F.col("attr_key") == "trace").alias("trace_events"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
    .filter(F.col("trace_events") > 1)
    .orderBy(F.col("version_avg_latency_ms").desc())
)
