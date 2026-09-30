from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy("unit_price_count", F.col("product_id").desc())
    .limit(5)
)
