from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)
