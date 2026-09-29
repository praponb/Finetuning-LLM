from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([2, 6]) & F.col("store_id").isin([1, 2, 3, 6]))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 212, "High").when(F.col("quantity") >= 51, "Medium").otherwise("Low"))
)

inventory_stats = (
    valid_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    inventory_stats
    .orderBy("store_id", "product_id")
    .limit(1)
)
