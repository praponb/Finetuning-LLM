from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method", "order_id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id")
w5 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method")
w_ordered = Window.partitionBy("method").orderBy("amount_share")
w8 = Window.partitionBy("method").orderBy(F.col("cumulative_amount").desc())
w9 = Window.partitionBy("method").orderBy(F.col("cumulative_amount_percent_rank").desc_nulls_first())
w10 = Window.partitionBy("method").orderBy(F.col("session_start_flag").desc_nulls_first())

with_windows = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.sum("amount").over(w_grp).alias("cumulative_amount"),
        F.sum("amount").over(w_running).alias("running_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

gaps = (
    with_windows
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w5))

kept_keys = (
    sessions_payments
    .select("method", "id")
    .exceptAll(
        df_payments
        .filter(F.col("paid_at") >= "2024-05-20")
        .select("method", "id")
    )
)

verified = (
    sessions_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "method",
        "cumulative_amount",
        "running_amount",
        "amount_share",
        "session_start_flag",
        "burst_id",
        F.min("amount_share").over(w6).alias("method_min_amount_share"),
        F.dense_rank().over(w_ordered).alias("amount_share_dense_rank"),
        F.percent_rank().over(w8).alias("cumulative_amount_percent_rank"),
    )
)

enriched = (
    scored
    .withColumn("running_amount_share", F.round((F.col("running_amount") / F.sum("running_amount").over(w6)), 4))
    .withColumn("cumulative_amount_share", F.round((F.col("cumulative_amount") / F.sum("cumulative_amount").over(w6)), 4))
    .withColumn("cumulative_amount_percent_rank_rank", F.rank().over(w9))
)

base = (
    enriched
    .filter(F.col("session_start_flag") >= 0)
    .select(
        "id",
        "method",
        "cumulative_amount",
        "amount_share",
        "session_start_flag",
        "burst_id",
        "method_min_amount_share",
        "cumulative_amount_percent_rank",
        "cumulative_amount_share",
        F.coalesce("cumulative_amount_percent_rank_rank", F.lit(0)).alias("cumulative_amount_percent_rank_rank_filled"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

metrics = (
    base
    .select(
        "cumulative_amount",
        "amount_share",
        "burst_id",
        "method_min_amount_share",
        "cumulative_amount_percent_rank",
        "cumulative_amount_share",
        F.rank().over(w10).alias("session_start_flag_rank"),
        F.count("session_start_flag").over(w6).alias("method_count_session_start_flag"),
    )
)

result = (
    metrics
    .withColumn("method_count_session_start_flag_tier", F.when(F.col("method_count_session_start_flag") >= 15, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["method_count_session_start_flag_tier", "burst_id"], ["method_count_session_start_flag_tier"]],
        "method_count_session_start_flag_tier",
        "burst_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("amount_share", 0.9).alias("amount_share_p90"),
        F.round(F.avg("method_count_session_start_flag"), 2).alias("avg_method_count_session_start_flag"),
        F.round(F.avg("session_start_flag_rank"), 2).alias("avg_session_start_flag_rank"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
    )
    .orderBy(F.col("avg_burst_id").desc())
)
