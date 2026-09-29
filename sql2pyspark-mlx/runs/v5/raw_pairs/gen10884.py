from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter((F.month("last_restock").isin([1, 6, 10]) & (F.col("product_id") == 3)) | (F.col("last_restock") <= "2023-06-01"))
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 0.9), 2))
)

agg = (
    recent_inventory
    .groupBy(
        F.when(F.col("quantity") >= 239, "high").when(F.col("quantity") >= 81, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = agg.orderBy("avg_quantity", F.col("last_last_restock").desc())
