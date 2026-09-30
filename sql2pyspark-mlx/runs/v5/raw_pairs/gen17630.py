from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 7, 9, 10]))
    .groupBy("store_id", "product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") >= 160)
    .orderBy("store_id", "product_id")
    .limit(10)
)
