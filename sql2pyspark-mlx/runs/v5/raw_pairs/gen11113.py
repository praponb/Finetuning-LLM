from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .groupBy(F.col("orders.channel"))
    .agg(
        F.round(F.stddev(F.col("order_items.unit_price")), 2).alias("stddev_unit_price"),
        F.count("*").alias("order_item_count"),
    )
    .orderBy("channel")
)
