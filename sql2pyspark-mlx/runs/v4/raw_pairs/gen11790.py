from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.name").like("D%") | F.col("orders.order_date").between("2024-01-02", "2025-05-01"))
    .select(
        F.col("shipments.delivered_date"),
        F.col("shipments.shipped_date"),
        F.col("orders.discount"),
        F.col("customers.email"),
    )
    .orderBy("discount")
)
