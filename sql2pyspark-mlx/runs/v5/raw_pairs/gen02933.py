from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 9) & (F.col("store_id").isin([3, 4, 5, 6]) | F.col("store_id").isin([1, 3, 5, 6])))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)
