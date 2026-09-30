from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 138) & F.col("product_id").isin([3, 5, 7, 8]) & (F.col("store_id") == 2))
    .select("id", "store_id", "last_restock", "quantity")
)
