from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id", "subscription_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("abs_balance").desc(), "id", "subscription_id")
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc(), "id", "subscription_id")
w4 = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id", "subscription_id")
w5 = Window.partitionBy("status", "account_type").orderBy("monthly_fee", "id", "subscription_id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-15").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.abs("balance").alias("abs_balance"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

matched = (
    cleaned
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept = (
    matched
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.hour(F.col("ch.txn_time")).between(7, 13)), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type") == "REFUND"), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "customer_id",
        "abs_balance",
        "status_filled",
        "subscription_id",
        "plan",
        F.lead("monthly_fee", 2).over(w).alias("next_monthly_fee"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.ntile(10).over(w4).alias("balance_bucket"),
        F.ntile(4).over(w5).alias("monthly_fee_bucket"),
    )
)
