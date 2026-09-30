from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

valid = (
    exploded
    .filter(F.col("latency_ms").between(895, 1462))
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        F.initcap(F.trim("key")).alias("key_title"),
    )
)

base = (
    valid
    .filter(F.col("key") == "region")
    .withColumn("key_service", F.concat("key", F.lit("/"), "service"))
)

remaining_keys = (
    base
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("service").isin(["checkout", "search"]))
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("latency_ms") >= 1870, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev_2"),
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_rows"),
    )
)
