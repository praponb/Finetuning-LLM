from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 8, 9]))
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .filter(F.col("min_quantity") >= 100)
)
