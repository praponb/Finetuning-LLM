from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6, 7, 10]))
    .select("id", "revenue", "units")
)
