from pyspark.sql import functions as F

flattened = df_app_logs.select("id", "service", "host", F.explode("context").alias("k", "v"))

valid = (
    flattened
    .filter(F.col("host") != "h4")
    .select(
        "*",
        F.concat_ws("-", "k", "host").alias("k_host_key"),
        F.when(F.col("k").isin(["region", "trace"]), "Core").otherwise("Other").alias("k_group"),
    )
)

cleaned = (
    valid
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "k",
        "v",
        "k_host_key",
        "k_group",
        F.lower("k").alias("k_lower"),
    )
)

totals = (
    cleaned
    .rollup("service", "host")
    .agg(
        F.count_if(F.col("service") == "checkout").alias("checkout_events"),
        F.count("*").alias("event_count"),
        F.grouping("service").alias("grouping_id"),
    )
    .filter(F.col("event_count") < 1)
)

result = totals.select("service", "host", "checkout_events", "event_count", "grouping_id")
