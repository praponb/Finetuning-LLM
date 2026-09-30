from pyspark.sql import functions as F

max_shipping_fee = df_orders.filter(F.col("order_date") > "2025-08-01").agg(F.max("shipping_fee")).first()[0]

result = (
    df_orders
    .filter(F.col("shipping_fee") >= max_shipping_fee)
    .select("id", "shipping_fee", "customer_id", "channel")
    .orderBy(F.col("customer_id").desc())
)
