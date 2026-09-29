from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2023-12-20")
    .select("id", "last_restock", "store_id", "quantity")
    .orderBy("id", "last_restock")
)
