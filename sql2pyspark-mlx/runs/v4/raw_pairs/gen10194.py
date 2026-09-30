from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter((F.col("country") == "TH") | (F.col("city") != "Tokyo"))
    .withColumn("country_title", F.initcap(F.trim("country")))
)

suppliers_stats = (
    src
    .groupBy("city")
    .agg(
        F.count("rating").alias("rating_count"),
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
    )
)

result = (
    suppliers_stats
    .select("city", "rating_count", "us_rating")
    .orderBy("rating_count", "city")
)
