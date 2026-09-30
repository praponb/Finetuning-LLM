from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("country") != "SG")
    .withColumn("rating_whole", F.floor("rating"))
)

rating_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.avg("rating_whole").alias("avg_rating_whole"),
    )
)

result = (
    rating_bucket_summary
    .select("rating_bucket", "avg_rating", "avg_rating_whole")
    .orderBy(F.col("avg_rating").desc(), F.col("rating_bucket").desc())
)
