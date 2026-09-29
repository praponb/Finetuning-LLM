from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 6]))
    .select("quantity")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") > "2023-12-03")
        .select("quantity")
    )
    .distinct()
)
