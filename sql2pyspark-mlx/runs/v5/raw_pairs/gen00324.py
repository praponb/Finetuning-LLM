from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts_day", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("latency_ms", "id", "k")
w_grp = Window.partitionBy("host").orderBy("latency_ms", "id", "k")
w4 = Window.orderBy("log_ts_day", "id", "k")
w5 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("latency_ms_count_2h", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("seq_no_2").asc_nulls_last())
w8 = Window.partitionBy("k").orderBy("running_sum_latency_ms_count_2h", "id", "k").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("k").orderBy("last4_min_seq_no", "id", "k")
w10 = Window.partitionBy("k").orderBy("running_min_latency_ms_count_2h", "id", "k").rowsBetween(-5, Window.currentRow)
w11 = Window.partitionBy("k").orderBy("prev_last4_count_seq_no", "id", "k").rowsBetween(-6, -1)
w12 = Window.partitionBy("k")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

valid = (
    flattened
    .filter((F.col("log_ts") >= F.lit("2025-06-01 06:00:00").cast("timestamp")) & F.col("k").isin(["version", "trace", "region"]))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "k",
        "attr_value",
        F.col("log_ts").cast("date").alias("log_ts_day"),
    )
)

scored = (
    valid
    .filter(F.col("host") == "h3")
    .select(
        "id",
        "k",
        "attr_value",
        "log_ts_day",
        F.last("k", ignorenulls=True).over(w).alias("last_known_k"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(4).over(w_grp).alias("latency_ms_bucket"),
        F.row_number().over(w4).alias("seq_no_2"),
        F.count("latency_ms").over(w5).alias("latency_ms_count_2h"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "k",
        "seq_no",
        "latency_ms_bucket",
        "seq_no_2",
        F.sum("latency_ms_count_2h").over(w6).alias("running_sum_latency_ms_count_2h"),
        F.min("latency_ms_count_2h").over(w6).alias("running_min_latency_ms_count_2h"),
        F.max("latency_ms_count_2h").over(w6).alias("running_max_latency_ms_count_2h"),
        F.percent_rank().over(w7).alias("seq_no_2_percent_rank"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "k",
        "seq_no_2",
        "running_min_latency_ms_count_2h",
        "running_max_latency_ms_count_2h",
        F.count("seq_no").over(w8).alias("last4_count_seq_no"),
        F.min("seq_no").over(w8).alias("last4_min_seq_no"),
        F.max("seq_no").over(w8).alias("last4_max_seq_no"),
    )
)

scored_app_logs = (
    metrics
    .select(
        "id",
        "k",
        "running_min_latency_ms_count_2h",
        "last4_min_seq_no",
        "last4_max_seq_no",
        F.lag("seq_no_2").over(w9).alias("prev_seq_no_2"),
        F.lag("last4_count_seq_no").over(w9).alias("prev_last4_count_seq_no"),
        F.lead("last4_count_seq_no").over(w9).alias("next_last4_count_seq_no"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

result = (
    scored_app_logs
    .select(
        "id",
        "k",
        "running_min_latency_ms_count_2h",
        "last4_min_seq_no",
        F.avg("next_last4_count_seq_no").over(w10).alias("next_last4_count_seq_no_avg_5"),
        F.avg("next_last4_count_seq_no").over(w11).alias("next_last4_count_seq_no_avg_6_prior"),
        F.round((F.col("last4_min_seq_no") / F.sum("last4_min_seq_no").over(w12)), 4).alias("last4_min_seq_no_share"),
    )
)
