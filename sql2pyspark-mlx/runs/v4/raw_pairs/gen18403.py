from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6, 10]))
    .groupBy("store_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
    .filter(F.col("stddev_revenue") > 42)
    .select("store_id", "sale_month", "stddev_revenue")
    .orderBy(F.col("store_id").desc())
)
