from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.month(F.col("s.shipped_date")).isin([3, 5, 12]))
    .select(
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("s.status"),
        F.col("s.cost"),
    )
    .orderBy(F.col("status").desc(), F.col("order_date").desc())
)
