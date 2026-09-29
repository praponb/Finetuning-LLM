from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]) & (F.col("store_id") == 5) & F.col("product_id").isin([7, 10]))
)
