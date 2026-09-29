from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2025-10-01")
    .select("product_id", "store_id")
    .orderBy(F.col("product_id").desc())
)
