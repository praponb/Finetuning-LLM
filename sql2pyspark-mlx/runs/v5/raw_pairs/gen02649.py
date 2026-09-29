from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("customer_id") == 15)
    .select(
        "*",
        F.concat("status", F.lit("/"), "account_type").alias("status_account_type"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)
