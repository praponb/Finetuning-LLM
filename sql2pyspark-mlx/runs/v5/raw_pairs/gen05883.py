from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "SG"]))
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.avg("rating").alias("avg_rating"),
    )
    .orderBy("rating_bucket")
)
