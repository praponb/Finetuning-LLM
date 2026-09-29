from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("product_id") == 3)
    .withColumn("units_whole", F.floor("units"))
)

agg = (
    recent_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = (
    agg
    .orderBy(F.col("store_id").desc(), F.col("stddev_revenue").desc())
    .limit(3)
)
