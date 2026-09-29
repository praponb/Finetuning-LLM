from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 132) & (F.col("product_id") == 1) & F.col("product_id").isin([1, 4, 5, 9]))
    .groupBy(
        F.when(F.col("quantity") >= 262, "high").when(F.col("quantity") >= 70, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)
