from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("revenue") < 2400)

store_sales_stats = (
    src
    .groupBy("store_id", F.year("sale_date").alias("sale_year"))
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = (
    store_sales_stats
    .select("store_id", "sale_year", "stddev_revenue")
    .orderBy(F.col("sale_year").desc())
)
