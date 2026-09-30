from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("cnt"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.unit_price_range") > 57)
    .select(F.col("o.store_id"), F.col("os.cnt"), F.col("os.unit_price_range"))
    .orderBy(F.col("cnt").desc())
)
