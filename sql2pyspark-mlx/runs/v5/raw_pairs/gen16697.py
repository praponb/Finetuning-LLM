from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 3200) & (F.col("revenue") >= 2000) & F.col("store_id").isin([1, 3, 4, 5]))
    .groupBy("product_id")
    .agg(F.min("revenue").alias("min_revenue"))
    .filter(F.col("min_revenue") >= 3300)
)
