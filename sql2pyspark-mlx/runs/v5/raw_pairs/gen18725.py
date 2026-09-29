from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 3)
    .select("id", "product_id")
    .orderBy(F.col("product_id").desc())
)
