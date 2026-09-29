from pyspark.sql import functions as F

matched_keys = (
    df_subscriptions
    .select("plan", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("plan") == "standard")
        .select("plan", "customer_id")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "plan", "start_date", "end_date", "monthly_fee", "status")
    .orderBy(F.col("status").desc())
)
