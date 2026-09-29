from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 212, "high").when(F.col("quantity") >= 97, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .orderBy(F.col("unique_products").desc(), "product_id", "quantity_bucket")
    .limit(1)
)
