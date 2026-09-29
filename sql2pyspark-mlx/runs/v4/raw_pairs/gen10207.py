from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").asc_nulls_last())
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w_running = Window.orderBy("opened_date", "id").rowsBetween(-2, -1)
w_grp = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.partitionBy("customer_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("balance_dense_rank", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy("balance_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-03-23"))) & F.col("customer_id").isNotNull() & F.col("account_type").isin(["SAVINGS", "CREDIT"]))
    .select(
        "*",
        F.upper("status").alias("status_upper"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.rank().over(w2).alias("balance_rank"),
        F.avg("balance").over(w_running).alias("balance_avg_2_prior"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

with_windows = (
    metrics
    .withColumn("running_event_seq", F.sum("event_seq").over(w5))
    .withColumn("balance_avg_2_prior_avg_6", F.avg("balance_avg_2_prior").over(w6))
    .withColumn("last_known_event_seq", F.last("event_seq", ignorenulls=True).over(w7))
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

latest_match_accounts = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

result = (
    latest_match_accounts
    .withColumn("balance_dense_rank_tier", F.when(F.col("balance_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .withColumn("balance_avg_2_prior_avg_6_tier", F.when(F.col("balance_avg_2_prior_avg_6") >= 69199, "PREMIUM").otherwise("STANDARD"))
    .rollup(
        "balance_dense_rank_tier",
        "activity_month",
        "balance_avg_2_prior_avg_6_tier",
    )
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.avg("balance_avg_2_prior_avg_6"), 2).alias("avg_balance_avg_2_prior_avg_6"),
        F.grouping_id().alias("grouping_level"),
    )
)
