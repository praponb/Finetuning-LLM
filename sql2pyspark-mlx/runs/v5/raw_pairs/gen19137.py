from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
    )
    .filter(F.col("pct_de") < 26)
    .orderBy("rating_bucket")
)
