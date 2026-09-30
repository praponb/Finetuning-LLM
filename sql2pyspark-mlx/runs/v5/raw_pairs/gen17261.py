from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 5]))
    .select("id", "store_id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.year("last_restock") == 2025)
        .select("id", "store_id", "quantity")
    )
)
