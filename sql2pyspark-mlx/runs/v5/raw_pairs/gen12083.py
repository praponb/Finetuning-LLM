from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") == 2024)
    .select("last_restock", "store_id", F.floor("quantity").alias("quantity_whole"))
)
