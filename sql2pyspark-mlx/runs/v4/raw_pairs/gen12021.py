from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

flattened = (
    df_carts
    .select("id", "customer_id", F.explode_outer("attributes").alias("k", "value"))
)

result = (
    flattened
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "k", F.col("max_order_amount"))
)
