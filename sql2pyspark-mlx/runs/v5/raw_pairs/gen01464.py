from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 6, 9]) & (F.col("product_id") == 2))
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.count("revenue").alias("revenue_count"))
    .orderBy("sale_month")
)
