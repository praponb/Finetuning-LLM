from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4]) & (F.col("sale_date") >= "2024-05-08") & (F.col("units") == 24))
    .groupBy("product_id", "store_id")
    .agg(F.count("*").alias("store_sale_count"))
)
