from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "attr_key", F.col("avg_subscription_monthly_fee"))
    .orderBy(
        F.col("avg_subscription_monthly_fee").desc_nulls_first(),
        F.col("id").desc(),
    )
)
