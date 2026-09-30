from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("s.status").isin(["LOST", "DELIVERED"]))
    .select(
        F.col("s.cost"),
        F.col("s.shipped_date"),
        F.col("s.order_id"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
    )
    .orderBy("shipped_date")
)
