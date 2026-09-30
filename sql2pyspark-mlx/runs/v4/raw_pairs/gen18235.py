from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 25)
    .groupBy("product_id")
    .agg(
        F.max("units").alias("max_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)
