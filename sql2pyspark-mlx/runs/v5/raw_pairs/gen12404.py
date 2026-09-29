from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") == 17) & (F.year("sale_date") == 2023))
    .groupBy("product_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy("product_id")
    .limit(5)
)
