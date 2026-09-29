from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "London")
    .groupBy(
        "country",
        "city",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
)
