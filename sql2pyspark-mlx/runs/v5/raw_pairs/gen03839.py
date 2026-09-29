from pyspark.sql import functions as F

kept_keys = (
    df_subscriptions
    .select("plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("plan")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = reconciled.select("plan", "monthly_fee", "status")
