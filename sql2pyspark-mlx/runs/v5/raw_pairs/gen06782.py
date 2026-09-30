from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.order_date") <= "2024-06-01")
    .groupBy(F.col("o.status"))
    .agg(
        F.min(F.col("s.delivered_date")).alias("first_delivered_date"),
        F.min(F.col("s.shipped_date")).alias("first_shipped_date"),
    )
)
