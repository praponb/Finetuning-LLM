from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "k")
w2 = Window.partitionBy("service").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("level_stddev_samp_latency_ms").desc_nulls_first(), "id", "k")
w5 = Window.partitionBy("k").orderBy("level_stddev_samp_latency_ms", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "v"),
    )
)

metrics = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "k",
        F.stddev_samp("latency_ms").over(w).alias("level_stddev_samp_latency_ms"),
        F.sum("latency_ms").over(w2).alias("cumulative_latency_ms"),
        F.sum("latency_ms").over(w3).alias("cumulative_latency_ms_2"),
    )
)

valid = (
    metrics
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "k",
        "level_stddev_samp_latency_ms",
        "cumulative_latency_ms",
        F.col("latency_ms").cast("double").alias("latency_ms_num"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("resolved_k"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("latest_rank_2", F.row_number().over(w5))
    .filter(F.col("latest_rank_2") == 1)
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "k",
        "level_stddev_samp_latency_ms",
    )
)

result = (
    ranked
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2624, "LARGE").otherwise("SMALL"))
    .rollup("host", "latency_ms_tier")
    .agg(
        F.sum(F.when(F.col("level") == "ERROR", F.col("latency_ms")).otherwise(0)).alias("error_latency_ms"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_count"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("host") == "h4", 1)), F.lit(0)), 2).alias("latency_ms_per_h4"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("latency_ms_per_h4") < 1000000)
)
