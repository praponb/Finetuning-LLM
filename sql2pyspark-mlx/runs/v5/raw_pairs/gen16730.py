from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
    .filter(F.col("stddev_revenue") > 20)
    .select("sale_month", "stddev_revenue")
    .orderBy(F.col("stddev_revenue").desc(), F.col("sale_month").desc())
)
