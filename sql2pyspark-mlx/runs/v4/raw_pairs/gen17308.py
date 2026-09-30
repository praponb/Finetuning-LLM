from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("s.order_id"),
        F.col("s.shipped_date"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("o.amount"),
    )
)
