from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select("id", "customer_id", "created_at", F.col("item.qty"), F.col("item.promo"))
)

result = (
    flattened
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "qty",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("sum_subscription_monthly_fee").desc())
)
