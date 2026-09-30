from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .select("store_id")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") > "2023-12-01")
        .select("store_id")
    )
    .orderBy("store_id")
)
