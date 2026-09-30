from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("merchant", "txn_type").orderBy("event_seq", "id")
w_running = Window.partitionBy("account_type").orderBy(F.col("row_seq").desc())
w6 = Window.partitionBy("account_type")
w7 = Window.partitionBy("merchant")
w8 = Window.partitionBy("merchant").orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("account_id").orderBy(F.col("prev_amount").desc(), "id")
w_recent = Window.partitionBy("account_type").orderBy("latest_rank", "id")

enriched = (
    df_transactions
    .withColumn("amount_avg_3", F.avg("amount").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w3))
)

metrics = (
    enriched
    .select(
        "*",
        F.lag("amount").over(w_hist).alias("prev_amount"),
        F.lag("amount_avg_3").over(w_hist).alias("prev_amount_avg_3"),
        F.lead("amount_avg_3").over(w_hist).alias("next_amount_avg_3"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.merchant",
        "a.amount_avg_3",
        "a.first_known_merchant",
        "a.prev_amount",
        "a.row_seq",
        F.col("b.account_type"),
        F.col("b.opened_date"),
    )
)

metrics_transactions = (
    with_dims
    .withColumn("row_seq_percent_rank", F.percent_rank().over(w_running))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w6)), 4))
    .withColumn("amount_share_2", F.round((F.col("amount") / F.sum("amount").over(w7)), 4))
)

scored = (
    metrics_transactions
    .filter(F.col("prev_amount").between(3100, 3500))
    .withColumn("running_count_amount_share_2", F.count("amount_share_2").over(w8))
    .withColumn("running_avg_amount_share_2", F.avg("amount_share_2").over(w8))
    .withColumn("running_max_amount_share_2", F.max("amount_share_2").over(w8))
    .withColumn("running_min_amount_share_2", F.min("amount_share_2").over(w8))
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
)

metrics_transactions_2 = (
    deduped
    .withColumn("prev_running_count_amount_share_2", F.lag("running_count_amount_share_2").over(w_recent))
    .withColumn("prev_amount_2", F.lag("amount").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent))
)

result = (
    metrics_transactions_2
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.status").alias("status"), F.col("s.merchant").alias("merchant"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("s.row_seq")), 2).alias("avg_row_seq"),
        F.min(F.col("s.running_min_amount_share_2")).alias("min_running_min_amount_share_2"),
        F.grouping_id().alias("grouping_level"),
    )
)
