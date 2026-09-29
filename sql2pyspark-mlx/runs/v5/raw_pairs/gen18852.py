from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5, 6]) & (F.col("units") <= 11))
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
    .orderBy(F.col("stddev_revenue").desc())
)
