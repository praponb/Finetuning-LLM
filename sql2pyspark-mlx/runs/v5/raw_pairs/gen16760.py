from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "UK") | (F.col("rating") <= 2.2))
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .filter(F.col("total_rating") < 8)
    .orderBy("rating_bucket")
)
