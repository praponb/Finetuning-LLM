from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 5, 6]) & F.month("last_restock").isin([1, 3, 5]))
    .select("id", "product_id")
)
