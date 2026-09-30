from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("stddev_revenue") < 80)
)
