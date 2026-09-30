from pyspark.sql import functions as F

min_last_restock = df_inventory.filter(F.col("store_id").isin([1, 3, 4, 6])).agg(F.min("last_restock")).first()[0]

result = (
    df_inventory
    .filter(F.col("last_restock") == min_last_restock)
    .select("id", "last_restock", "product_id", "store_id")
    .orderBy("store_id")
)
