from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

kept_keys = (
    df_accounts
    .select("id", "account_type")
    .intersectAll(
        df_accounts
        .filter(F.col("status").isNull())
        .select("id", "account_type")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "opened_date", F.col("sum_subscription_monthly_fee"))
)
