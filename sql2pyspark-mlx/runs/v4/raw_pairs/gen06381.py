from pyspark.sql import functions as F

valid_products = df_products.filter(~F.col("product_name").like("S%"))

launch_month_summary = (
    valid_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = launch_month_summary
