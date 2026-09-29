from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

matched_keys = (
    df_sessions
    .select("device", "id")
    .intersectAll(
        df_sessions
        .filter(F.to_date("started_at") >= "2025-02-11")
        .select("device", "id")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
