from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.flash_count") > 67)
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("os.flash_count"),
    )
    .orderBy("flash_count")
)
