from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "product_id")
    .intersect(
        df_inventory
        .filter(F.col("store_id").isin([1, 3, 5, 6]))
        .select("id", "product_id")
    )
)
