from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.status").isin(["CANCELLED", "COMPLETED", "SHIPPED"]) & F.month(F.col("s.shipped_date")).isin([2, 7, 9]))
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("s.delivered_date"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.when(F.col("s.cost") >= 74, "High").when(F.col("s.cost") >= 41, "Medium").otherwise("Low").alias("cost_tier"),
    )
)
