from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5, 6]))
    .groupBy("product_id", F.month("sale_date").alias("sale_month"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.sum("units").alias("total_units"),
        F.countDistinct("revenue").alias("unique_products"),
    )
    .filter(F.col("unique_products") > 2)
    .drop("unique_products")
)
