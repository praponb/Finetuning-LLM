from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

flattened = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

result = (
    flattened
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "code",
        F.when(F.col("code").rlike("^(SPRING10|VIP)$"), "EXTENDED").when(F.col("code").rlike("^(BUNDLE|WELCOME5)$"), "PRIORITY").otherwise("GROUP_A").alias("code_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy("customer_id")
)
