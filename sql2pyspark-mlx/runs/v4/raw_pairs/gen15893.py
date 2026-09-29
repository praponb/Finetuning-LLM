from pyspark.sql import functions as F

result = (
    df_suppliers
    .cube(
        "country",
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
    )
)
