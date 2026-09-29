from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") == 2025)
    .select("id", "store_id")
    .intersect(
        df_inventory
        .filter(F.col("last_restock") < "2024-11-26")
        .select("id", "store_id")
    )
    .orderBy(F.col("store_id").desc())
)
