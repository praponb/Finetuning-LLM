from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "host").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "k")
w2 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_last(), "id", "k")
w3 = Window.partitionBy("service").orderBy("row_num", "id", "k").rowsBetween(-2, Window.currentRow)

lines = (
    df_app_logs
    .select("id", "service", "host", "latency_ms", F.explode("context").alias("k", "v"))
)

latest = (
    lines
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    deduped
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "k",
        "row_num",
        F.count("latency_ms").over(w3).alias("last3_count_latency_ms"),
        F.min("latency_ms").over(w3).alias("last3_min_latency_ms"),
        F.avg("latency_ms").over(w3).alias("last3_avg_latency_ms"),
    )
)

filtered = (
    with_windows
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("version"))))
    .select(
        "id",
        "k",
        "row_num",
        "last3_count_latency_ms",
        "last3_avg_latency_ms",
        F.trim("k").alias("clean_k"),
        F.length("k").alias("k_length"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

result = (
    filtered
    .withColumn("k_length_tier", F.when(F.col("k_length") >= 80, "HIGH").otherwise("LOW"))
    .rollup("k", "k_length_tier")
    .agg(
        F.avg("last3_count_latency_ms").alias("avg_last3_count_latency_ms"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.size(F.collect_set("k")).alias("num_ks_2"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_count"),
        F.percentile_approx("last3_avg_latency_ms", 0.5).alias("last3_avg_latency_ms_p50"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("k_length_tier").desc_nulls_last())
)
