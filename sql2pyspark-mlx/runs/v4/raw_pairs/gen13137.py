from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 6, 11]) & (F.col("last_restock") < "2024-08-01"))
    .select("id", "store_id")
)
