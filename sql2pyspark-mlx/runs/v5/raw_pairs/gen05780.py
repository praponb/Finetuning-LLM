from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("rating").alias("rating_count"),
    )
    .filter(F.col("rating_count") >= 4)
    .orderBy(F.col("total_rating").desc(), "rating_count")
)
