from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("stddev_rating") >= 73)
    .orderBy(F.col("stddev_rating").desc())
)
