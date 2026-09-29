from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .groupBy(
        F.when(F.col("city").isin(["Berlin", "Singapore"]), "core").otherwise("other").alias("city_group"),
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
    .filter(F.col("pct_singapore") < 52)
    .orderBy(F.col("pct_singapore").desc())
)
