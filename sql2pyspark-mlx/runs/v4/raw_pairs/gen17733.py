from pyspark.sql import functions as F

shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

not_flagged = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left_anti")
)

result = (
    not_flagged
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select("id", "store_id", "discount", F.col("min_order_shipping_fee"))
    .orderBy("id")
)
