from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 2) & (F.col("store_id") == 1) & F.col("store_id").isin([1, 2, 5]))
    .select("id", "store_id")
)
