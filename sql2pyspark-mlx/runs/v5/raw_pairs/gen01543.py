from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w_hist = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key")
w_ordered = Window.partitionBy("attr_key").orderBy("latest_rank", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("attr_key")
w6 = Window.partitionBy("attr_key").orderBy("prev_status_code", "id", "attr_key")
w7 = Window.orderBy("latest_rank_times_attr_key_stddev_samp_prev_status_code", "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

deduped = (
    flattened
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .filter(F.col("status_code") == 413)
    .select(
        "*",
        F.lag("status_code").over(w_hist).alias("prev_status_code"),
        F.lead("status_code").over(w_hist).alias("next_status_code"),
        F.lag("latency_ms").over(w_hist).alias("prev_latency_ms"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.ntile(4).over(w3).alias("latency_ms_bucket"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w_hist))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "attr_key",
        "value",
        "dedup_rank",
        "prev_status_code",
        "prev_latency_ms",
        "latest_rank",
        F.avg("prev_status_code").over(w_ordered).alias("last4_avg_prev_status_code"),
        F.min("prev_status_code").over(w_ordered).alias("last4_min_prev_status_code"),
        F.count("prev_status_code").over(w_ordered).alias("last4_count_prev_status_code"),
        F.stddev_samp("prev_status_code").over(w_running).alias("attr_key_stddev_samp_prev_status_code"),
    )
)

base = (
    enriched
    .filter(F.col("attr_key").isNotNull() & (F.col("prev_status_code") != 294))
    .select(
        "id",
        "attr_key",
        "value",
        "dedup_rank",
        "prev_status_code",
        "latest_rank",
        "last4_avg_prev_status_code",
        "last4_min_prev_status_code",
        "last4_count_prev_status_code",
        "attr_key_stddev_samp_prev_status_code",
        (F.col("latest_rank") * F.col("attr_key_stddev_samp_prev_status_code")).alias("latest_rank_times_attr_key_stddev_samp_prev_status_code"),
    )
)

enriched_app_logs = (
    base
    .select(
        "id",
        "attr_key",
        "latest_rank",
        "last4_avg_prev_status_code",
        "attr_key_stddev_samp_prev_status_code",
        F.lag("latest_rank_times_attr_key_stddev_samp_prev_status_code").over(w6).alias("prev_latest_rank_times_attr_key_stddev_samp_prev_status_code"),
        F.lag("last4_avg_prev_status_code").over(w6).alias("prev_last4_avg_prev_status_code"),
        F.row_number().over(w6).alias("row_seq"),
        F.lead("attr_key_stddev_samp_prev_status_code", 1).over(w7).alias("next_attr_key_stddev_samp_prev_status_code"),
    )
)

grouped = (
    enriched_app_logs
    .groupBy(
        F.when(F.col("next_attr_key_stddev_samp_prev_status_code") >= 426, "PREMIUM").otherwise("STANDARD").alias("next_attr_key_stddev_samp_prev_status_code_tier"),
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.round(F.sum("next_attr_key_stddev_samp_prev_status_code") / F.nullif(F.count(F.when(F.col("attr_key") == "trace", 1)), F.lit(0)), 2).alias("next_attr_key_stddev_samp_prev_status_code_per_trace"),
        F.round(F.sum("attr_key_stddev_samp_prev_status_code") / F.nullif(F.count(F.when(F.col("attr_key") == "trace", 1)), F.lit(0)), 2).alias("attr_key_stddev_samp_prev_status_code_per_trace"),
        F.sum(F.when(F.col("attr_key") == "trace", F.col("next_attr_key_stddev_samp_prev_status_code")).otherwise(0)).alias("trace_next_attr_key_stddev_samp_prev_status_code"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
    )
)

result = (
    grouped
    .select(
        "next_attr_key_stddev_samp_prev_status_code_tier",
        "trace_rows",
        "attr_key_stddev_samp_prev_status_code_per_trace",
        "trace_next_attr_key_stddev_samp_prev_status_code",
        "num_rows",
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
