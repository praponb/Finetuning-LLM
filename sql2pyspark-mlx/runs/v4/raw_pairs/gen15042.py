from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.orderBy("credit_limit")
w3 = Window.partitionBy("country")
w_running = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())

matched = (
    df_accounts
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

matched_keys = (
    matched
    .select("account_type", "id")
    .subtract(
        df_accounts
        .filter(F.col("opened_date").between("2023-06-01", "2023-09-01"))
        .select("account_type", "id")
    )
)

reconciled = (
    matched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.segment"), F.col("c.country"))
)

matched_keys_2 = (
    joined
    .select("id")
    .intersectAll(
        df_accounts
        .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
        .select("id")
    )
)

reconciled_accounts = (
    joined
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled_accounts
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4))
    .withColumn("credit_limit_percent_rank", F.percent_rank().over(w2))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4))
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w_running))
)

kept = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type") == "REFUND"), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "account_type",
        "status",
        "plan",
        "monthly_fee",
        "country",
        "credit_limit_share",
        "monthly_fee_share",
        "monthly_fee_percent_rank",
    )
)
