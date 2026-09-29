from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("cost") > 36).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.amount") > 780)
    .select(F.col("o.id"), F.col("o.channel"))
)
