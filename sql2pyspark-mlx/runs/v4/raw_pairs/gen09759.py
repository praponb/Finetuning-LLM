from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type", "customer_id").orderBy("balance_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("balance_avg_2", "id")
w7 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("running_balance_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

scored = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_6_prior"),
        F.ntile(10).over(w2).alias("balance_bucket"),
        F.avg("balance").over(w3).alias("balance_avg_2"),
    )
)

with_windows = (
    scored
    .withColumn("running_balance_avg_6_prior", F.sum("balance_avg_6_prior").over(w4))
    .withColumn("running_balance", F.sum("balance").over(w5))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

enriched = (
    eligible
    .select(
        "*",
        F.row_number().over(w6).alias("event_seq"),
        F.sum("running_balance_avg_6_prior").over(w7).alias("cumulative_running_balance_avg_6_prior"),
        F.first("balance_avg_2", ignorenulls=True).over(w8).alias("first_known_balance_avg_2"),
    )
)

attributed = (
    enriched
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.opened_date") >= F.col("sub.start_date")) & (F.col("a.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_last = (
    attributed
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

totals = (
    with_last
    .cube(
        F.when(F.col("first_known_balance_avg_2") >= 74024, "PREMIUM").otherwise("STANDARD").alias("first_known_balance_avg_2_tier"),
        F.when(F.col("balance_avg_2") >= 85345, "HIGH").otherwise("LOW").alias("balance_avg_2_tier"),
        "subscription_id",
    )
    .agg(
        F.percentile_approx("balance_bucket", [0.25, 0.5, 0.75]).alias("balance_bucket_quartiles"),
        F.avg(F.when(F.col("status") == "CLOSED", F.col("balance"))).alias("closed_avg_balance"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("closed_avg_balance") >= 0)
)

result = (
    totals
    .select(
        "first_known_balance_avg_2_tier",
        "balance_avg_2_tier",
        "subscription_id",
        "balance_bucket_quartiles",
        "closed_avg_balance",
        "num_plans",
        "checking_balance",
        "loan_count",
        "cnt",
    )
    .orderBy(F.col("first_known_balance_avg_2_tier").desc_nulls_last(), "loan_count")
)
