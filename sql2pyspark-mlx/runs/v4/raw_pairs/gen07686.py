from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level").orderBy("latency_ms", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms_whole").asc_nulls_first())
w3 = Window.partitionBy("level", "attr_key").orderBy("latency_ms_whole", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

base = (
    flattened
    .filter(F.col("latency_ms") > 863)
    .withColumn("latency_ms_whole", F.floor("latency_ms"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("OTHER")))
)

scored = (
    base
    .select(
        "*",
        F.row_number().over(w_recent).alias("event_seq"),
        F.dense_rank().over(w2).alias("latency_ms_whole_dense_rank"),
        F.sum("latency_ms_whole").over(w3).alias("cumulative_latency_ms_whole"),
    )
)

result = (
    scored
    .groupBy("attr_key")
    .agg(
        F.percentile_approx("cumulative_latency_ms_whole", [0.25, 0.5, 0.75]).alias("cumulative_latency_ms_whole_quartiles"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.percentile_approx("latency_ms_whole", [0.25, 0.5, 0.75]).alias("latency_ms_whole_quartiles"),
    )
)
