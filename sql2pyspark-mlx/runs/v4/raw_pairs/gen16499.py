from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([15, 32, 39]))
    .groupBy("order_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("quantity").alias("quantity_count"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
    .orderBy("quantity_count")
)
