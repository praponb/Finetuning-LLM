from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 6]))
    .select("product_id")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([1, 4, 5, 6]))
        .select("product_id")
    )
    .distinct()
    .orderBy(F.col("product_id").desc())
)
