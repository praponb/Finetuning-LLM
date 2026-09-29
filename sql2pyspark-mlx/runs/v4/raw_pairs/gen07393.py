from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(((F.col("sale_date") > "2024-02-27") & (F.col("units") >= 19)) | (F.col("product_id") == 3))
)

summary = (
    recent_store_sales
    .groupBy("product_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)

result = summary
