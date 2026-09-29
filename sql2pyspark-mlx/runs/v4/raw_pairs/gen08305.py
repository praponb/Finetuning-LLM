from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("orders.shipping_fee") > 17)
    .groupBy(F.col("customers.is_active"))
    .agg(
        F.max(F.col("shipments.shipped_date")).alias("last_shipped_date"),
        F.countDistinct(F.col("shipments.cost")).alias("unique_orders"),
    )
    .orderBy(F.col("unique_orders").desc(), "is_active")
    .limit(20)
)
