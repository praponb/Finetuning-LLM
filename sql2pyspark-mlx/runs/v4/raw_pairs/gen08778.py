from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 7, 10]))
    .select("quantity", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 3, 5]))
        .select("quantity", "product_id")
    )
    .distinct()
    .orderBy(F.col("product_id").desc())
)
