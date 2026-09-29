from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("host").orderBy("dedup_rank", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

deduped = (
    exploded
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "latency_ms", "k", "dedup_rank")
)

valid = (
    latest
    .filter(F.col("k").isNotNull() & (F.col("k") != "region"))
    .select(
        "*",
        (F.col("dedup_rank") * F.col("latency_ms")).alias("dedup_rank_times_latency_ms"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("dedup_rank_times_latency_ms") >= 71, "HIGH").otherwise("LOW").alias("dedup_rank_times_latency_ms_tier"),
        "k",
    )
    .agg(
        F.count_if(F.col("k") == "trace").alias("trace_events"),
        F.sum(F.when(F.col("k") == "trace", F.col("dedup_rank_times_latency_ms")).otherwise(0)).alias("trace_dedup_rank_times_latency_ms"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
    .filter(F.col("trace_dedup_rank_times_latency_ms") > 0)
    .orderBy(F.col("latency_ms_bit_and").desc_nulls_last())
)
