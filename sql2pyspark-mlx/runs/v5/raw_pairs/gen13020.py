from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.min("rating").alias("min_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("min_rating") >= 2.5)
    .select("city", "min_rating", "stddev_rating")
    .orderBy(F.col("stddev_rating").desc())
)
