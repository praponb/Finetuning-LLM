from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("stock_qty") >= 238)

agg = (
    valid_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    agg
    .filter(F.col("stddev_cost") < 20)
    .select("launch_quarter", "stddev_cost")
    .orderBy("launch_quarter")
)
