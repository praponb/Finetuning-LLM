from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 6]))
    .select("product_id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([1, 3, 4, 6]))
        .select("product_id", "quantity")
    )
    .distinct()
    .orderBy("product_id")
)
