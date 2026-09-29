from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"))
    .agg(F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"))
    .orderBy(F.col("stddev_revenue").desc(), "launch_month")
    .limit(10)
)
