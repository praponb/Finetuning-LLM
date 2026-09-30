from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 105)
    .select("id", "store_id", "last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") > "2023-05-28")
        .select("id", "store_id", "last_restock")
    )
    .orderBy("store_id")
)
