from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

result = (
    lines
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", F.col("min_order_discount"))
)
