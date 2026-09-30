from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") < 1900)
    .select("sale_year", "stddev_revenue", "avg_revenue")
    .orderBy(F.col("avg_revenue").desc(), "sale_year")
    .limit(5)
)
