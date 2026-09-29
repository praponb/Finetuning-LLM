from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter(F.col("unit_price").between(100, 370) & (F.col("unit_price") <= 370))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

order_items_stats = (
    filtered_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
)

result = (
    order_items_stats
    .filter(F.col("stddev_quantity") > 64)
    .select("order_id", "stddev_quantity", "unique_orders")
    .orderBy("unique_orders")
)
