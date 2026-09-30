from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 7, 9]) & (F.col("product_id") == 1))
    .select("id", "store_id")
    .orderBy(F.col("id").desc(), F.col("store_id").desc())
)
