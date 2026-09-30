from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("status")
w3 = Window.orderBy(F.col("balance").asc_nulls_last())
w4 = Window.partitionBy("transaction_merchant", "status").orderBy("opened_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("transaction_txn_type_2", "status").orderBy("opened_date", "id", "subscription_id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("transaction_txn_type_2").orderBy("balance", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

with_window = (
    latest_match
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

metrics = (
    with_window
    .filter(F.col("transaction_txn_type") == "REFUND")
    .withColumn("status_count_balance", F.count("balance").over(w_hist))
    .withColumn("balance_percent_rank", F.percent_rank().over(w3))
    .withColumn("last_known_transaction_merchant", F.last("transaction_merchant", ignorenulls=True).over(w4))
)

latest_match_accounts = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type").alias("transaction_txn_type_2"),
    )
)

with_dims = (
    latest_match_accounts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.credit_limit"), F.col("p.name"), F.col("p.email"))
)

metrics_accounts = (
    with_dims
    .select(
        "*",
        F.avg("monthly_fee").over(w6).alias("last4_avg_monthly_fee"),
        F.sum("monthly_fee").over(w6).alias("last4_sum_monthly_fee"),
        F.max("monthly_fee").over(w6).alias("last4_max_monthly_fee"),
        F.min("monthly_fee").over(w6).alias("last4_min_monthly_fee"),
        F.sum("monthly_fee").over(w7).alias("running_monthly_fee"),
    )
)

result = (
    metrics_accounts
    .groupBy(
        F.when(F.col("subscription_id") >= 9, "HIGH").otherwise("LOW").alias("subscription_id_tier"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("balance_percent_rank", 0.95).alias("balance_percent_rank_p95"),
        F.count(F.when(F.col("transaction_merchant") == "Shell", 1)).alias("shell_count"),
        F.percentile_approx("last4_max_monthly_fee", 0.9).alias("last4_max_monthly_fee_p90"),
    )
)
