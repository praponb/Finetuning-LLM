from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

matched_keys = (
    df_accounts
    .select("status")
    .subtract(
        df_accounts
        .filter(F.col("opened_date") >= "2023-06-01")
        .select("status")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "account_type",
        "balance",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("balance").asc_nulls_last())
)
