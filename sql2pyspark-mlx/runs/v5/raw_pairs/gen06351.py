from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("last_restock").between("2024-03-22", "2025-05-28") & (F.col("store_id").isin([2, 3, 5, 6]) | F.col("product_id").isin([1, 7])))
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 1.15), 2))
)

agg = (
    filtered_inventory
    .groupBy(
        "store_id",
        F.when(F.col("quantity_adjusted") >= 67, "high").when(F.col("quantity_adjusted") >= 15, "mid").otherwise("low").alias("quantity_adjusted_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = (
    agg
    .select("store_id", "quantity_adjusted_bucket", "unique_stores")
    .orderBy(F.col("unique_stores").desc())
)
