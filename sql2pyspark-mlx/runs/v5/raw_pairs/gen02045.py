from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("carrier") == "FedEx").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("o.customer_id"))
    .orderBy(F.col("shipping_fee").desc(), F.col("id").desc())
)
