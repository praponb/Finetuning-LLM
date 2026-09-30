from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .filter(F.col("avg_rating") > 33)
    .orderBy("rating_range", "rating_bucket")
)
