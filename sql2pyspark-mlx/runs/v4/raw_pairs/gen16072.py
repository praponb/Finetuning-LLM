from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") > 210) & F.col("store_id").isin([3, 4]) & F.month("last_restock").isin([5, 6, 12]))
    .select("id", "product_id", "store_id")
    .orderBy(F.col("store_id").desc())
)
