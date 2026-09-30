from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "brand")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") < 42)
    .select("category", "brand", "stddev_cost")
    .orderBy(F.col("stddev_cost").desc())
)
