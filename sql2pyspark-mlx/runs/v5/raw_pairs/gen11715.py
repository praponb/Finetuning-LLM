from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 5]) & F.col("product_id").isin([1, 4, 6, 9]))
)
