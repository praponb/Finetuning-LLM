from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("revenue").isNull())

summary = (
    recent_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(F.min("sale_date").alias("min_sale_date"))
)

result = summary.select("sale_month", "min_sale_date")
