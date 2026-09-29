from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-06-25", "2025-04-01") & (F.col("product_id") == 5))
    .select("id", "product_id")
)
