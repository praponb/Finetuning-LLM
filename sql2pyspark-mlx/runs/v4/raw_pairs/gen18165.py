from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)
