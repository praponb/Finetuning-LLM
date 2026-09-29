from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("host")
w_part_2 = Window.partitionBy("host", "k").orderBy("latency_ms", "id", "k")
w3 = Window.partitionBy("k").orderBy("latency_ms")

exploded = (
    df_app_logs
    .select("id", "host", "level", "latency_ms", F.explode("context").alias("k", "v"))
)

valid = (
    exploded
    .filter(F.col("k").isNotNull() & F.col("host").isin(["h3", "h4", "h2"]))
    .select(
        "id",
        "host",
        "latency_ms",
        "k",
        F.trim("level").alias("clean_level"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.avg("latency_ms").over(w_part).alias("host_avg_latency_ms"),
        F.ntile(10).over(w_part_2).alias("latency_ms_bucket"),
        F.rank().over(w3).alias("latency_ms_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("latency_ms") >= 2489, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
        "host",
    )
    .agg(
        F.round(F.sum("latency_ms_bucket") / F.nullif(F.count(F.when(F.col("k") == "trace", 1)), F.lit(0)), 2).alias("latency_ms_bucket_per_trace"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows"),
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
    )
)
