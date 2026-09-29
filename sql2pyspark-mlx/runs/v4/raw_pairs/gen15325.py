from pyspark.sql import functions as F

result = (
    df_inventory
    .select("product_id", "store_id", "last_restock")
    .orderBy(F.col("product_id").desc(), F.col("store_id").desc())
)
