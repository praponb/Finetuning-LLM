from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 4, 6, 7]) & F.col("store_id").isin([1, 2, 3, 4]))
    .orderBy(F.col("id").desc())
)
