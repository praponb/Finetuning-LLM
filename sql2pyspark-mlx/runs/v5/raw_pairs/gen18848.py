from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.count("*").alias("num_products"),
    )
    .filter(F.col("stddev_cost") > 33)
    .select("brand", "stddev_cost", "num_products")
    .orderBy(F.col("stddev_cost").desc())
)
