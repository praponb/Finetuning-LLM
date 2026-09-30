from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 7, 11]))
    .groupBy(
        F.when(F.col("quantity") >= 184, "high").when(F.col("quantity") >= 47, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.count("*").alias("num_inventory"),
    )
    .orderBy("min_last_restock")
)
