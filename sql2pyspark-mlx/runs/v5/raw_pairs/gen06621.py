from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("max_rating") < 2.1)
    .orderBy("city", F.col("max_rating").desc(), "rating_bucket")
    .limit(1)
)
