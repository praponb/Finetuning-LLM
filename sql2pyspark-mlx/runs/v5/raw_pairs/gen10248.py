from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

lines = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

result = (
    lines
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "tag", F.col("sum_subscription_monthly_fee"))
    .orderBy(F.col("tag").desc_nulls_last(), "sum_subscription_monthly_fee")
)
