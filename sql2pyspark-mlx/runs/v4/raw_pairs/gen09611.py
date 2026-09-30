from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 39) & (F.col("order_id").isin([1, 14, 29, 36]) | (F.col("quantity") != 4)))
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") < 3.0)
    .drop("max_quantity")
)
