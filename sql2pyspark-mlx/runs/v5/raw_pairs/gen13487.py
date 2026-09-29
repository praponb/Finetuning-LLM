from pyspark.sql import functions as F

base = df_store_sales.filter((F.col("units") >= 18) & F.col("store_id").isin([1, 2, 3, 6]))

store_sales_stats = (
    base
    .groupBy(F.quarter("sale_date").alias("sale_quarter"), "product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = store_sales_stats.orderBy("max_revenue")
