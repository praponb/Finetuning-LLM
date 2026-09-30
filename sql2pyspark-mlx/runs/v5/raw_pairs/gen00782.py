from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter((F.col("sale_date") > "2024-09-01") & F.col("product_id").isin([7, 8]) & F.col("product_id").isin([1, 5, 6, 10]))
)

summary = (
    valid_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = summary
