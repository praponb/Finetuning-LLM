from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("avg_rating") > 46)
    .select("rating_bucket", "avg_rating", "cnt")
    .orderBy(F.col("rating_bucket").desc(), "cnt")
)
