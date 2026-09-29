from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.max("revenue").alias("max_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("stddev_revenue") < 32)
)
