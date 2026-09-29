from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.customer_id").isin([7, 18, 26, 31]) & F.month(F.col("o.order_date")).isin([1, 3, 12]))
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.status"),
        F.col("o.shipping_fee"),
    )
)
