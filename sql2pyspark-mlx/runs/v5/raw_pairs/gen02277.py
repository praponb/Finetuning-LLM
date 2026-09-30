from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .withColumn("country_title", F.initcap(F.trim("country")))
)

suppliers_stats = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
)

result = suppliers_stats.select("rating_bucket", "min_rating", "singapore_count")
