from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]))
    .select("store_id", "last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("product_id").isin([3, 4, 5]))
        .select("store_id", "last_restock")
    )
    .distinct()
    .orderBy(F.col("last_restock").desc())
)
