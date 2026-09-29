from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 6, 7]) & F.col("store_id").isin([1, 3, 6]) & F.col("product_id").isin([3, 7, 9, 10]))
    .select("id", "last_restock", "quantity", "store_id")
    .orderBy("store_id")
)
