from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select("store_id")
    .unionByName(
        df_inventory
        .filter(F.col("quantity").between(97, 121))
        .select("store_id")
    )
    .distinct()
)
