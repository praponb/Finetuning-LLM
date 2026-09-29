from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("max_rating") < 2.6)
    .select("rating_bucket", "avg_rating", "max_rating")
)
