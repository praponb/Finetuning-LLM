from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("s.cost").isNotNull() | ~F.col("s.carrier").isin(["Kerry", "DHL"]))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.order_id"),
        F.col("s.cost"),
        F.col("o.order_date"),
        F.col("o.status"),
    )
)
