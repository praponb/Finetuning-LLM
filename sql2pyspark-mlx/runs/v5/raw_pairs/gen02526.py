from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "coupon_codes",
        "k",
        "attr_value",
        F.when(F.col("k").rlike("^(weight_kg|gift)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("k_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("k_zone").desc_nulls_last())
)
