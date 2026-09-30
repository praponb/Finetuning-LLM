from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("rating").alias("rating_count"),
    )
    .orderBy("city", F.col("stddev_rating").desc(), "rating_bucket")
    .limit(3)
)
