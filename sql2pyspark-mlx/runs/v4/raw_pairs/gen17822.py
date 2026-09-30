from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5, 8, 10]) | (F.col("last_restock") > "2025-02-01"))
)

summary = (
    recent_inventory
    .groupBy(
        F.when(F.col("quantity") >= 243, "high").when(F.col("quantity") >= 107, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
)

result = summary.filter(F.col("min_quantity") > 70)
