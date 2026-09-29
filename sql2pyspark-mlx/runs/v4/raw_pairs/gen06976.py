from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 6]) & (F.col("last_restock") > "2023-10-01") & F.col("product_id").isin([2, 5, 8]))
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 1.1), 2))
)
