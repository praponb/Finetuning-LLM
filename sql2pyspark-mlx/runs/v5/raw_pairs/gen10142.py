from pyspark.sql import functions as F

recent_order_items = df_order_items.filter((F.col("order_id") == 1) | F.col("product_id").isNull())

order_id_summary = (
    recent_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    order_id_summary
    .filter(F.col("unit_price_count") < 7)
    .orderBy(F.col("unit_price_count").desc())
)
