from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .select(
        F.col("shipments.cost"),
        F.col("shipments.delivered_date"),
        F.col("orders.amount"),
        F.col("orders.channel"),
        F.col("customers.name"),
        F.col("customers.age"),
    )
)
