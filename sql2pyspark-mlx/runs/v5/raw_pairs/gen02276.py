from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

flattened = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    flattened
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "qty", "price", "promo", F.col("avg_order_discount"))
    .orderBy(F.col("qty").desc())
)
