from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "right")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("order_items.product_id") == 17)
    .groupBy(
        F.when(F.col("orders.channel").isin(["partner", "store"]), "priority").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev(F.col("order_items.unit_price")), 2).alias("stddev_unit_price"),
    )
)
