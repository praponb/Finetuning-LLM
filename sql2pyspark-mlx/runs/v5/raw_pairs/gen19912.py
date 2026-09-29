from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 10]) & (F.col("revenue") <= 1200))
    .select("id", "revenue", "units", "product_id")
)
