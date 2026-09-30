from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id", "key")
w2 = Window.orderBy("key_length", "id", "key").rowsBetween(-5, -1)
w3 = Window.partitionBy("service", "key").orderBy("key_length_avg_5_prior", "id", "key")
w4 = Window.orderBy("latency_ms_bucket", "id", "key").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("key_length_avg_5_prior", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("key").orderBy("key_length_avg_5_prior", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("service").orderBy("rank_in_group", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("key", "service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

cleaned = (
    flattened
    .filter(F.col("level") == "ERROR")
    .select(
        "*",
        F.length("key").alias("key_length"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

scored = (
    cleaned
    .filter(F.col("latency_ms") != 611)
    .select(
        "id",
        "service",
        "key",
        "resolved_key",
        F.ntile(4).over(w).alias("latency_ms_bucket"),
        F.avg("key_length").over(w2).alias("key_length_avg_5_prior"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .withColumn("latency_ms_bucket_avg_2", F.avg("latency_ms_bucket").over(w4))
    .withColumn("first_known_key", F.first("key", ignorenulls=True).over(w5))
    .withColumn("running_key_length_avg_5_prior", F.sum("key_length_avg_5_prior").over(w_hist))
    .withColumn("cumulative_latency_ms_bucket", F.sum("latency_ms_bucket").over(w7))
    .withColumn("key_max_key_length_avg_5_prior", F.max("key_length_avg_5_prior").over(w_ordered))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("cumulative_latency_ms_bucket") >= 40, "LARGE").otherwise("SMALL").alias("cumulative_latency_ms_bucket_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms_bucket", [0.25, 0.5, 0.75]).alias("latency_ms_bucket_quartiles"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("cumulative_latency_ms_bucket_tier").desc())
)
