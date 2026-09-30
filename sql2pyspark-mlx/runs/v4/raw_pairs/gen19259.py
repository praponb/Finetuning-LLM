from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

remaining_keys = (
    df_subscriptions
    .select("customer_id", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee") > 90)
        .select("customer_id", "id")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last(), "end_date")
)
