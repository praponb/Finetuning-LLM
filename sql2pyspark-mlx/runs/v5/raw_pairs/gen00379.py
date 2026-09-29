from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3]) & (F.col("product_id").isin([1, 4, 6, 8]) | (F.col("quantity") <= 80)))
    .select("id", "product_id")
    .orderBy("id")
)
