from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w_running = Window.partitionBy("key").orderBy(F.col("service_max_latency_ms").asc_nulls_last())
w4 = Window.partitionBy("service").orderBy(F.col("latency_ms_cume_dist").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

filtered = (
    flattened
    .filter(F.col("key").isin(["region", "version"]))
    .select(
        "*",
        F.substring("key", 1, 2).alias("key_prefix"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull() & (F.col("service") == "billing"))
    .select(
        "*",
        F.when(F.col("key").isin(["version", "region"]), "Tier 1").otherwise("Other").alias("key_group"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("resolved_key"),
    )
)

filtered_app_logs = (
    cleaned
    .filter(F.col("key").isNotNull())
    .select("*", F.upper("level").alias("level_upper"))
)

cleaned_app_logs = (
    filtered_app_logs
    .filter(F.col("key").isNotNull() & (F.col("service") == "checkout"))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "key",
        "key_prefix",
        "key_clean",
        "key_group",
        "resolved_key",
        "level_upper",
        F.concat("service", F.lit(" "), "key").alias("service_key"),
    )
)

with_windows = (
    cleaned_app_logs
    .withColumn("service_max_latency_ms", F.max("latency_ms").over(w))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
)

scored = (
    with_windows
    .withColumn("service_max_latency_ms_rank", F.rank().over(w_running))
    .withColumn("latency_ms_cume_dist_rank", F.rank().over(w4))
)

result = (
    scored
    .select(
        "level",
        "key_prefix",
        "key_clean",
        "key_group",
        "resolved_key",
        "level_upper",
        "service_key",
        "service_max_latency_ms",
        "latency_ms_share",
        F.when(F.col("level").rlike("^(DEBUG|WARN)$"), "CORE").when(F.col("level").rlike("^(INFO|ERROR)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("level_zone"),
    )
    .orderBy(F.col("level_zone").desc())
)
