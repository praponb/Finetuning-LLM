from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 146) & F.col("quantity").between(81, 179) & F.col("store_id").isin([3, 4, 6]))
    .groupBy("product_id", "store_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") >= 110)
    .orderBy(F.col("product_id").desc())
)
