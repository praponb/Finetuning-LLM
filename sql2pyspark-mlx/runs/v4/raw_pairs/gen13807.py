from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel").isin(["web", "mobile", "partner"]))
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.amount"),
        F.col("o.channel"),
        F.col("o.discount"),
    )
)
