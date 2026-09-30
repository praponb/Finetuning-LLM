from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

lines = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

result = (
    lines
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "element",
        F.when(F.col("element").rlike("^(SPRING10|VIP)$"), "PRIORITY").when(F.col("element").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_A").otherwise("EXTENDED").alias("element_zone"),
        F.col("max_subscription_monthly_fee"),
    )
)
