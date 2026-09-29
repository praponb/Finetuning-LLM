from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") < 1150) & (F.length("product_name") > 8))
    .groupBy("category")
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
    .filter(F.col("stddev_price") < 23)
    .orderBy("category")
)
