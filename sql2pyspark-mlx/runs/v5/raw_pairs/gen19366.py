from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 2)
    .select("product_id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([3, 4, 5, 6]))
        .select("product_id")
    )
)
