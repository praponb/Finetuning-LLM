from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.cost").isNull())
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("o.discount"),
    )
)
