from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") >= 4) & (F.col("quantity").isNotNull() | (F.col("unit_price") > 260)))
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy("max_quantity")
)
