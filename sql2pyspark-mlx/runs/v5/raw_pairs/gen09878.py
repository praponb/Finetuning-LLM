from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 92)
    .select("id", "last_restock", "store_id")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") == 215)
        .select("id", "last_restock", "store_id")
    )
    .distinct()
)
