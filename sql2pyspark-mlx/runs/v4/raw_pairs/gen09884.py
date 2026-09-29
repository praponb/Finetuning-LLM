from pyspark.sql import functions as F

base = df_store_sales.filter(F.month("sale_date").isin([2, 8, 9]))

store_sales_stats = (
    base
    .groupBy("product_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = store_sales_stats.select("product_id", "store_sale_count")
