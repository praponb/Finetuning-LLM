from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5, 6]) & F.col("product_id").isin([1, 4, 8, 9]))
    .groupBy("store_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.count("revenue").alias("revenue_count"),
    )
    .orderBy("stddev_units")
)
