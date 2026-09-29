from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .filter((F.col("shipments.status") != "IN_TRANSIT") & (F.col("customers.city") != "London"))
    .select(
        F.col("shipments.order_id"),
        F.col("orders.status"),
        F.col("orders.channel"),
        F.col("customers.segment"),
    )
    .orderBy("segment", F.col("order_id").desc())
)
