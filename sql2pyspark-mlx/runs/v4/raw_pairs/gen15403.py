from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.order_id").isin([13, 28, 40]))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.cost"),
        F.col("s.delivered_date"),
        F.col("o.order_date"),
        F.col("o.channel"),
    )
    .orderBy(F.col("cost").desc(), "channel")
)
