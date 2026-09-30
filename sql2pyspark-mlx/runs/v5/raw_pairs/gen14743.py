from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 8, 10]) & F.col("store_id").isin([2, 4, 5]) & (F.year("last_restock") == 2024))
)
