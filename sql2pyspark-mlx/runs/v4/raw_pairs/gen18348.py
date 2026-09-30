from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-11-01", "2025-07-25"))
    .select("store_id", "last_restock", "product_id")
)
