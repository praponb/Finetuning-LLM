from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.partitionBy("host", "key")
w3 = Window.partitionBy("host").orderBy(F.col("latency_ms_share_rounded").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

cleaned = (
    flattened
    .filter((F.col("key").eqNullSafe(F.lit("trace"))) & F.col("key").isNotNull())
    .withColumn("key_filled", F.coalesce("key", F.lit("Unknown")))
    .withColumn("latency_ms_filled", F.coalesce("latency_ms", F.lit(0)))
    .withColumn("key_filled_2", F.coalesce("key", F.lit("UNKNOWN")))
)

with_windows = (
    cleaned
    .filter(F.col("key") != "version")
    .select(
        "id",
        "host",
        F.rank().over(w).alias("latency_ms_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
    )
)

filtered = (
    with_windows
    .filter(F.col("latency_ms_share") <= 0.32)
    .withColumn("clean_host", F.trim("host"))
)

cleaned_app_logs = (
    filtered
    .filter(F.col("host").isin(["h3", "h1"]))
    .select("*", F.round("latency_ms_share", 2).alias("latency_ms_share_rounded"))
)

scored = (
    cleaned_app_logs
    .select(
        "latency_ms_share",
        "clean_host",
        F.rank().over(w3).alias("latency_ms_share_rounded_rank"),
    )
)

result = scored.select("latency_ms_share", "latency_ms_share_rounded_rank")
