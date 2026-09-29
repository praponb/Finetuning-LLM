from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3]))
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.min("sale_date").alias("min_sale_date"),
    )
)
