from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5]))
    .select("id", "quantity", "store_id")
    .unionByName(
        df_inventory
        .filter(F.col("store_id") == 4)
        .select("id", "quantity", "store_id")
    )
    .orderBy("id")
)
