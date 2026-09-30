from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(82, 85) & (F.col("last_restock") < "2025-07-31"))
    .orderBy("last_restock", F.col("store_id").desc())
)
