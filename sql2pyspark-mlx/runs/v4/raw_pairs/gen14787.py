from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 1.9)
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("avg_rating") > 31)
    .select("rating_bucket", "avg_rating", "total_rating")
)
