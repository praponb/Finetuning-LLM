from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

remaining_keys = (
    df_subscriptions
    .select("plan", "customer_id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select("plan", "customer_id")
    )
)

verified = (
    df_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "plan", "end_date", F.col("sum_account_balance"))
    .orderBy(F.col("end_date").desc_nulls_last(), "customer_id")
)
