from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 5]) & (F.col("product_id").isin([2, 6, 7]) | (F.col("last_restock") > "2023-04-01")))
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)
