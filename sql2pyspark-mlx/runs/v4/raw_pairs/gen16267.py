from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 10]))
    .select("id", "product_id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") > "2023-08-21")
        .select("id", "product_id", "store_id")
    )
    .orderBy("store_id")
)
