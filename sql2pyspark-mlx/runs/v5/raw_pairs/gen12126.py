from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("quantity") <= 88) & (F.col("last_restock") < "2024-02-07")) | F.col("product_id").isin([1, 3, 6]))
    .select("id", "store_id", "quantity")
    .orderBy("quantity")
)
