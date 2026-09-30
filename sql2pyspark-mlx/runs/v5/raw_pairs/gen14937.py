from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.returned_count") > 4)
    .select(F.col("o.id"), F.col("o.channel"), F.col("os.returned_count"))
)
