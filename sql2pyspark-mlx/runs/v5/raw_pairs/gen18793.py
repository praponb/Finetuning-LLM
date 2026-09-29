from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 214) | F.col("product_id").isin([3, 8, 10]))
    .select("quantity", "last_restock", "store_id")
)
