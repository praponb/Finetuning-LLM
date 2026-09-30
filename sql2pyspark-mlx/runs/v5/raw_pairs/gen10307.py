from pyspark.sql import functions as F

with_window = (
    df_accounts
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "subscription_id",
        "plan",
        "monthly_fee",
    )
    .orderBy("opened_date", "subscription_id")
)
