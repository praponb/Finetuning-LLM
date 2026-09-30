from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max("rating").alias("max_rating"))
    .filter(F.col("max_rating") > 2.8)
    .select("city", "rating_bucket", "max_rating")
    .orderBy(F.col("max_rating").desc())
)
