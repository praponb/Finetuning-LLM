from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("balance_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("balance_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("account_type").orderBy("balance", "id")
w5 = Window.partitionBy("status").orderBy(F.col("row_seq").desc(), "id", "subscription_id")
w6 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-09-13"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.col("balance").cast("int").alias("balance_int"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.sum("balance").over(w_grp).alias("running_balance"),
        F.sum("balance").over(w2).alias("running_balance_2"),
        F.avg("balance").over(w3).alias("balance_avg_3_prior"),
    )
)

enriched = (
    with_windows
    .filter(F.col("running_balance") < 120162)
    .select(
        "*",
        F.lead("running_balance").over(w_ordered).alias("next_running_balance"),
        F.lag("balance").over(w_ordered).alias("prev_balance"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

attributed = (
    enriched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

deduped = (
    attributed
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"), F.col("c.amount"))
)

result = (
    latest_match
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.city").alias("city"),
        F.col("p.country").alias("country"),
        F.col("src.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.subscription_id")).alias("max_subscription_id"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
