from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 5, 9]) & (F.col("quantity") < 233) & (F.col("product_id") == 4))
    .select("last_restock", "store_id", "product_id")
)
