from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id", "plan").orderBy(F.col("balance_filled").desc(), "id", "subscription_id")
w_recent = Window.partitionBy("customer_id").orderBy("monthly_fee", "id", "subscription_id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("prev_monthly_fee").desc_nulls_last(), "id", "subscription_id")
w5 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-09-25").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.lag("balance_filled").over(w).alias("prev_balance_filled"),
        F.lag("balance").over(w).alias("prev_balance"),
        F.row_number().over(w).alias("seq_no"),
    )
)

attributed = (
    enriched
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

latest = (
    attributed
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "subscription_id",
        "plan",
        "monthly_fee",
    )
)

enriched_accounts = (
    latest
    .filter(F.col("subscription_id") >= 6)
    .select(
        "*",
        F.lead("monthly_fee").over(w_recent).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w_recent).alias("prev_monthly_fee"),
        F.lead("balance").over(w_recent).alias("next_balance"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

ranked = (
    enriched_accounts
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        "s.next_balance",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

result = (
    with_latest
    .select("id", "balance", "subscription_id", "monthly_fee", "next_balance")
    .orderBy(F.col("next_balance").desc())
)
