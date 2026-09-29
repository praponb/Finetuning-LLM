from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") > 12) & F.lower("supplier_name").like("%uro%"))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .orderBy(F.col("avg_rating").desc(), "rating_bucket", "city")
    .limit(50)
)
