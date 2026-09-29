from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id", F.year("sale_date").alias("sale_year"))
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)
