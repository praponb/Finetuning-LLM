from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 7]))
    .select("product_id")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([2, 4, 5, 6]))
        .select("product_id")
    )
    .distinct()
    .orderBy("product_id")
)
