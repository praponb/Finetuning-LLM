from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 5, 6]) & F.col("product_id").isin([3, 5, 7, 8]))
    .withColumn("sale_date_label", F.date_format("sale_date", "MMM yyyy"))
)

store_sales_stats = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(F.min("revenue").alias("min_revenue"))
)

result = (
    store_sales_stats
    .filter(F.col("min_revenue") >= 2100)
    .select("product_id", "min_revenue")
)
