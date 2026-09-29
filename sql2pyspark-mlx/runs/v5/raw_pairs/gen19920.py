from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"), "brand")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") > 49)
    .orderBy("brand")
)
