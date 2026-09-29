from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 6.5)
    .drop("max_quantity")
    .orderBy(F.col("total_count").desc())
)
