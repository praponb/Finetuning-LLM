from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.6, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
)
