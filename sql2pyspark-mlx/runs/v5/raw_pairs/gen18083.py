from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.discount") <= 0.26)
    .select(
        F.col("s.cost"),
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("o.discount"),
        F.col("o.order_date"),
    )
)
