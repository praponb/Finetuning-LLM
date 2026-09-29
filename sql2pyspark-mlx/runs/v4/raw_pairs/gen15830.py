from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_first())
w_ordered = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("level").orderBy("latency_ms", "id", "attr_key").rowsBetween(-2, -1)
w4 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w5 = Window.partitionBy("level", "attr_key").orderBy(F.col("status_code_rank").desc())
w6 = Window.orderBy(F.col("latency_ms").asc_nulls_last())
w7 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms_rank").desc())
w8 = Window.partitionBy("level", "attr_key").orderBy("latency_ms_rank", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("attr_key").orderBy("status_code_rank", "id", "attr_key")
w10 = Window.orderBy("latency_ms", "id", "attr_key")
w11 = Window.partitionBy("attr_key")
w12 = Window.partitionBy("level").orderBy(F.col("latency_ms_avg_2_prior").asc_nulls_first(), "id", "attr_key")
w13 = Window.partitionBy("level", "host").orderBy(F.col("status_code").desc(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

ranked = (
    flattened
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    ranked
    .withColumn("status_code_rank", F.rank().over(w_ordered))
    .withColumn("latency_ms_avg_2_prior", F.avg("latency_ms").over(w3))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w4))
)

enriched = (
    with_windows
    .withColumn("status_code_rank_dense_rank", F.dense_rank().over(w5))
    .withColumn("latency_ms_rank", F.rank().over(w6))
)

with_windows_app_logs = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w7).alias("latency_ms_rank_dense_rank"),
        F.last("latency_ms_rank", ignorenulls=True).over(w8).alias("last_known_latency_ms_rank"),
        F.ntile(3).over(w9).alias("status_code_rank_bucket"),
        F.lead("status_code_percent_rank", 2).over(w10).alias("next_status_code_percent_rank"),
        F.avg("latency_ms_avg_2_prior").over(w11).alias("attr_key_avg_latency_ms_avg_2_prior"),
    )
)

ranked_app_logs = (
    with_windows_app_logs
    .withColumn("row_num", F.row_number().over(w12))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest = (
    ranked_app_logs
    .withColumn("latest_rank", F.row_number().over(w13))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("attr_key")
    .agg(
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.percentile_approx("latency_ms_avg_2_prior", 0.95).alias("latency_ms_avg_2_prior_p95"),
    )
)
