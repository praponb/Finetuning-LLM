from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 6]) & F.col("store_id").isin([1, 3, 5]))
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("last_restock").alias("last_last_restock"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("total_quantity").desc())
)
