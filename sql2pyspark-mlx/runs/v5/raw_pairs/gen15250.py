from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

exploded = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("entry.qty"),
        F.col("entry.promo"),
    )
)

result = (
    exploded
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "qty",
        "promo",
        F.when(F.col("promo").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_A").when(F.col("promo").rlike("^(SPRING10|VIP)$"), "CORE").otherwise("OTHER_ZONE").alias("promo_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("promo").desc())
)
