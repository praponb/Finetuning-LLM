from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

kept_keys = (
    flattened
    .select("service")
    .exceptAll(
        df_app_logs
        .filter(F.col("host") != "h2")
        .select("service")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("service") == "auth")
    .withColumn("service_upper", F.upper("service"))
)

filtered = (
    base
    .filter(F.col("k").isin(["trace", "region"]))
    .withColumn("level_prefix", F.substring("level", 1, 2))
    .withColumn("k_clean", F.coalesce("k", F.lit("DIRECT")))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("status_code") >= 405, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("level") == "ERROR", 1)), F.lit(0)), 2).alias("status_code_per_error"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
    )
    .filter(F.col("event_count") < 1000)
    .orderBy(F.col("status_code_tier").desc())
)
