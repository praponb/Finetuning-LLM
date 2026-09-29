from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
    )
    .orderBy(F.col("unique_countrys").desc())
)
