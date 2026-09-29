from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 7) & F.col("product_id").isin([1, 2]))
    .groupBy("store_id")
    .agg(F.count("revenue").alias("revenue_count"))
    .orderBy("store_id")
)
