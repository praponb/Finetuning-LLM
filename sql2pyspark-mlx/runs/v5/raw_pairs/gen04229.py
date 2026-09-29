from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("k").orderBy("latency_ms", "id", "k")
w3 = Window.partitionBy("level")
w_running = Window.partitionBy("k").orderBy("latency_ms")
w5 = Window.orderBy("service_min_latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

scored = (
    flattened
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        F.min("latency_ms").over(w).alias("service_min_latency_ms"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "latency_ms",
        "k",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w3)), 4).alias("seq_no_share"),
        F.dense_rank().over(w_running).alias("latency_ms_dense_rank"),
        F.sum("seq_no").over(w5).alias("running_seq_no"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("latency_ms") < 818)
    .select(
        "*",
        (F.col("seq_no_share") + F.col("latency_ms_dense_rank")).alias("seq_no_share_plus_latency_ms_dense_rank"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        (F.col("seq_no_share_plus_latency_ms_dense_rank") * F.col("latency_ms_dense_rank")).alias("seq_no_share_plus_latency_ms_dense_rank_times_latency_ms_dense_rank"),
        F.round((F.col("seq_no_share") * 1.15), 2).alias("adjusted_seq_no_share"),
    )
)

result = (
    cleaned_app_logs
    .groupBy("k")
    .agg(
        F.percentile_approx("seq_no_share_plus_latency_ms_dense_rank", 0.9).alias("seq_no_share_plus_latency_ms_dense_rank_p90"),
        F.count_if(F.col("k") == "region").alias("region_events"),
    )
    .filter(F.col("seq_no_share_plus_latency_ms_dense_rank_p90") > 0)
    .orderBy("seq_no_share_plus_latency_ms_dense_rank_p90")
)
