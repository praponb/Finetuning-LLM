from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_first())
w4 = Window.partitionBy("account_id")
w5 = Window.partitionBy("txn_type")
w6 = Window.partitionBy("account_id").orderBy(F.col("amount_cume_dist").desc_nulls_last())
w7 = Window.partitionBy("merchant").orderBy("amount_cume_dist_dense_rank", "id")
w8 = Window.partitionBy("merchant").orderBy("amount_cume_dist_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("prev_amount", "id").rowsBetween(-5, -1)
w10 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("next_last_known_amount_share", "id")
w_running_2 = Window.orderBy(F.col("amount_cume_dist_dense_rank").desc(), "id")
w13 = Window.orderBy("next_amount", "id")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("merchant") != "Lazada"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w_recent).alias("amount_bucket"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
    )
)

metrics_transactions = (
    metrics
    .select(
        "id",
        "amount",
        "merchant",
        "is_flagged",
        "merchant_filled",
        F.max("amount_bucket").over(w4).alias("account_id_max_amount_bucket"),
        F.round((F.col("last_known_amount") / F.sum("last_known_amount").over(w5)), 4).alias("last_known_amount_share"),
        F.first("amount", ignorenulls=True).over(w2).alias("first_known_amount"),
        F.dense_rank().over(w6).alias("amount_cume_dist_dense_rank"),
    )
)

with_windows = (
    metrics_transactions
    .filter(F.col("amount").between(1100, 1200))
    .select(
        "id",
        "account_id_max_amount_bucket",
        "last_known_amount_share",
        "first_known_amount",
        "amount_cume_dist_dense_rank",
        F.lead("last_known_amount_share").over(w7).alias("next_last_known_amount_share"),
        F.lag("amount").over(w7).alias("prev_amount"),
        F.lead("amount").over(w7).alias("next_amount"),
        F.row_number().over(w7).alias("seq_no"),
        F.sum("amount").over(w8).alias("running_amount"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.avg("running_amount").over(w_running).alias("running_amount_avg_5_prior"),
        F.first("account_id_max_amount_bucket", ignorenulls=True).over(w10).alias("first_known_account_id_max_amount_bucket"),
        F.lead("next_amount", 1).over(w11).alias("next_next_amount"),
        F.ntile(10).over(w_running_2).alias("amount_cume_dist_dense_rank_bucket"),
        F.row_number().over(w13).alias("event_seq"),
    )
)

stats = (
    enriched
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("cnt"),
        F.max("prev_amount").alias("max_prev_amount"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.count("*").alias("event_count"),
        F.percentile_approx("first_known_amount", [0.25, 0.5, 0.75]).alias("first_known_amount_quartiles"),
    )
)

result = (
    stats
    .select("seq_no", "cnt", "seq_no_quartiles", "event_count")
    .orderBy(F.col("event_count").desc_nulls_first(), "seq_no")
)
