from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
    .filter(F.col("stddev_price") > 73)
    .orderBy("category")
)
