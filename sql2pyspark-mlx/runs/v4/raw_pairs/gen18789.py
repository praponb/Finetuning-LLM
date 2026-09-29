from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy(F.col("rating_bucket").desc())
)
