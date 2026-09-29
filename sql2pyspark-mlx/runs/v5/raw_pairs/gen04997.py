from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("stddev_revenue") < 54)
    .orderBy("sale_quarter")
)
