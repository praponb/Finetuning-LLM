from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w_ordered = Window.partitionBy("attr_key").orderBy("latency_ms_percent_rank", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("attr_key")
w_part = Window.partitionBy("attr_key").orderBy(F.col("last4_sum_status_code_avg_2").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

with_windows = (
    lines
    .filter(F.col("latency_ms") > 1737)
    .select(
        "id",
        "attr_key",
        F.avg("status_code").over(w).alias("status_code_avg_2"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.max("status_code_avg_2").over(w_ordered).alias("last4_max_status_code_avg_2"),
        F.avg("status_code_avg_2").over(w_ordered).alias("last4_avg_status_code_avg_2"),
        F.min("status_code_avg_2").over(w_ordered).alias("last4_min_status_code_avg_2"),
        F.sum("status_code_avg_2").over(w_ordered).alias("last4_sum_status_code_avg_2"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "attr_key",
        "last4_max_status_code_avg_2",
        "last4_avg_status_code_avg_2",
        F.stddev_samp("last4_min_status_code_avg_2").over(w4).alias("attr_key_stddev_samp_last4_min_status_code_avg_2"),
        F.row_number().over(w_part).alias("seq_no"),
        F.round((F.col("last4_min_status_code_avg_2") / F.sum("last4_min_status_code_avg_2").over(w4)), 4).alias("last4_min_status_code_avg_2_share"),
    )
)

daily = (
    scored
    .groupingSets([["attr_key", "seq_no"], ["attr_key"], []], "attr_key", "seq_no")
    .agg(
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_count"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.round(F.avg("last4_min_status_code_avg_2_share"), 2).alias("avg_last4_min_status_code_avg_2_share"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_seq_no") < 1000000)
)

result = (
    daily
    .select(
        "seq_no",
        "trace_count",
        "avg_last4_min_status_code_avg_2_share",
        "region_events",
        "num_attr_keys",
    )
    .orderBy(F.col("avg_last4_min_status_code_avg_2_share").desc(), "trace_count")
)
