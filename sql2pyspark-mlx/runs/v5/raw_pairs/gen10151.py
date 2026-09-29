from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.discount") < 0.15)
    .groupBy(
        F.month(F.col("orders.order_date")).alias("order_month"),
        F.col("orders.customer_id"),
    )
    .agg(
        F.max(F.col("shipments.shipped_date")).alias("latest_shipped_date"),
        (F.max(F.col("shipments.cost")) - F.min(F.col("shipments.cost"))).alias("cost_range"),
    )
    .orderBy("order_month", F.col("customer_id").desc())
)
