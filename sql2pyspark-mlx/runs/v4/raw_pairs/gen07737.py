from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("city").isNull() & ((F.col("city") == "Singapore") | (F.col("rating") > 3.6)))
)

city_summary = (
    src
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.count("rating").alias("rating_count"),
    )
)

result = (
    city_summary
    .select("city", "rating_bucket", "london_count", "rating_count")
    .orderBy("city", "rating_bucket")
    .limit(5)
)
