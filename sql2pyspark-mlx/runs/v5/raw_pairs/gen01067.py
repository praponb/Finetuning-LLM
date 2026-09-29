from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

with_dims = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.name").like("D%"))
    .select("s.id", "s.customer_id", "s.plan", "s.monthly_fee", F.col("b.age"))
)

result = (
    with_dims
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "age",
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy("max_subscription_monthly_fee", "age")
)
