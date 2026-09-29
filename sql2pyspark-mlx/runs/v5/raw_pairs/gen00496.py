from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "store_id", "last_restock")
    .orderBy(F.col("last_restock").desc())
)
