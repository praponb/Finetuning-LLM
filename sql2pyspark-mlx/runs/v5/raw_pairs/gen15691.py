from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 1) & (F.col("store_id") == 2))
    .orderBy(F.col("product_id").desc())
)
