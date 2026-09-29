from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 24)
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
