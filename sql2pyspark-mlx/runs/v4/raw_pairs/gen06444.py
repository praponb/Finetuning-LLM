from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 9]) & F.col("store_id").isin([2, 3, 5]))
    .rollup("store_id", F.year("sale_date").alias("sale_year"))
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy(F.col("sale_year").desc())
)
