from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(73, 188) & (F.col("product_id") == 1) & (F.col("quantity") != 238))
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 260, "high").when(F.col("quantity") >= 110, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("num_inventory"),
    )
    .orderBy(F.col("total_quantity").desc())
)
