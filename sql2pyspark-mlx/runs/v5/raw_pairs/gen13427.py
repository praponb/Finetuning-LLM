from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.in_transit_count") >= 5)
    .select(F.col("o.id"), F.col("o.order_date"), F.col("os.in_transit_count"))
)
