from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter((F.col("product_id") == 7) & F.col("product_id").isin([1, 8, 10]))
)

store_sales_stats = (
    recent_store_sales
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = store_sales_stats.orderBy(F.col("product_id").desc(), "latest_sale_date")
