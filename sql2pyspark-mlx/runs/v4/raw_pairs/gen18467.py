from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 4, 5, 6]))
    .groupBy("product_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .filter(F.col("revenue_range") >= 31)
    .select("product_id", "sale_month", "revenue_range")
    .orderBy("sale_month", "product_id")
    .limit(50)
)
