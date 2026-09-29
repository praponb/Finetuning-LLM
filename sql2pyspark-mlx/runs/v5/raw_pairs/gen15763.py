from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 8]) & (F.col("store_id").isin([1, 3, 4, 5]) | F.col("product_id").isin([1, 4, 8, 10])))
    .select("id", "store_id", "product_id", "last_restock")
    .orderBy(F.col("store_id").desc(), F.col("last_restock").desc(), "id")
    .limit(3)
)
