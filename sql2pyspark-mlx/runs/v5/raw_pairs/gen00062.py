from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.count("units").alias("units_count"),
    )
    .filter(F.col("stddev_revenue") >= 76)
    .select("sale_month", "stddev_revenue", "units_count")
)
