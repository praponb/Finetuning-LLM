from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") == 2024)
    .select("last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([3, 5, 6]))
        .select("last_restock")
    )
    .distinct()
)
