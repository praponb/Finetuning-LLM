from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.delivered_count") > 8)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("os.kerry_count"),
        F.col("os.delivered_count"),
    )
    .orderBy(F.col("id").desc())
)
