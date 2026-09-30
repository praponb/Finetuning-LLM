from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 8)
    .select("id", "last_restock", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([1, 2, 8, 10]))
        .select("id", "last_restock", "store_id")
    )
)
