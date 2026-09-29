from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 6) & (F.col("quantity") != 111))
    .orderBy(F.col("store_id").desc())
)
