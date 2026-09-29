from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 9]) & F.month("last_restock").isin([1, 8, 12]))
    .select("last_restock", "quantity")
    .orderBy(F.col("quantity").desc())
)
