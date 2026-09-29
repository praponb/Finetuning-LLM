from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(F.col("s.id"), F.col("s.order_id"), F.col("s.cost"), F.col("o.channel"))
    .orderBy("cost", F.col("channel").desc(), "id")
    .limit(50)
)
