from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("account_type", "merchant").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("account_id", "txn_type").orderBy("balance", "id")
w4 = Window.partitionBy("txn_type").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.orderBy("running_count_amount", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("txn_type").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("event_seq").desc(), "id")
w8 = Window.partitionBy("year_start").orderBy(F.col("event_seq_bucket_per_refund").desc())

filtered = (
    df_transactions
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-04-07")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isin([19, 23, 25, 38]))
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.amount",
        "src.merchant",
        "src.txn_time_hour",
        F.col("d.opened_date"),
        F.col("d.account_type"),
        F.col("d.balance"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    latest
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time_hour",
        "opened_date",
        "balance",
        F.sum("amount").over(w2).alias("running_sum_amount"),
        F.count("amount").over(w2).alias("running_count_amount"),
        F.min("amount").over(w2).alias("running_min_amount"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "account_id",
        "txn_type",
        "opened_date",
        "balance",
        "running_sum_amount",
        "running_count_amount",
        "running_min_amount",
        F.lead("balance").over(w_running).alias("next_balance"),
        F.lag("balance").over(w_running).alias("prev_balance"),
        F.lead("txn_time_hour").over(w_running).alias("next_txn_time_hour"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .withColumn("running_count_amount_avg_2_prior", F.avg("running_count_amount").over(w5))
    .withColumn("running_prev_balance", F.sum("prev_balance").over(w6))
    .withColumn("event_seq_bucket", F.ntile(3).over(w7))
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("year", "opened_date").alias("year_start"),
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.round(F.sum("event_seq_bucket") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("event_seq_bucket_per_refund"),
        F.percentile_approx("next_txn_time_hour", [0.25, 0.5, 0.75]).alias("next_txn_time_hour_quartiles"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
    )
    .filter(F.col("event_seq_bucket_per_refund") > 0)
    .withColumn("event_seq_bucket_per_refund_percent_rank", F.percent_rank().over(w8))
)
