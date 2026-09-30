from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.7)
    .cube(
        "country",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "SG", F.col("rating")).otherwise(0)).alias("sg_rating"),
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
    .orderBy(F.col("rating_bucket").desc(), F.col("sg_rating").desc())
)
