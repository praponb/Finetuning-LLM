from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([4, 6, 9]))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.max("units").alias("max_units"),
    )
    .filter(F.col("max_units") < 36)
    .select("store_id", "stddev_revenue", "max_units")
    .orderBy(F.col("stddev_revenue").desc())
)
