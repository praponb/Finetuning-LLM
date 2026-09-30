from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 8, 9]) & (F.col("quantity") <= 114) & (F.col("quantity") > 160))
    .select("id", "product_id", "last_restock", "store_id")
)
