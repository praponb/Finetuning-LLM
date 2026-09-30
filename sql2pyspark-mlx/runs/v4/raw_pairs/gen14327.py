from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .select("store_id", "quantity", "last_restock")
    .orderBy("last_restock")
)
