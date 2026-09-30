from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3, 9]) & F.col("store_id").isin([1, 3, 5, 6]))
    .select("id", "units", "store_id", "revenue")
    .orderBy(F.col("store_id").desc(), "id")
    .limit(3)
)
