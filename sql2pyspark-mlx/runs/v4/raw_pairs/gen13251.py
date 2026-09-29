from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 6]) & ((F.col("product_id") == 3) | (F.col("revenue") < 2100)))
    .select("id", "product_id")
    .orderBy(F.col("id").desc())
)
