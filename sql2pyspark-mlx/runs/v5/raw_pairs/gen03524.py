from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 4, 7]))
    .select("last_restock", "store_id")
)
