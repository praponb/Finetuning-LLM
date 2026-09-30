from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 30)
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("unit_price_count").desc())
)
