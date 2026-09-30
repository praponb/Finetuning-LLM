from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.month("shipped_date").isin([3, 10, 12])).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.amount") < 590)
    .select(F.col("o.id"), F.col("o.channel"), F.col("o.amount"))
)
