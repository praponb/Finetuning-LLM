from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(~F.col("country").isin(["US", "UK"]))
    .select("*", F.substring("city", 1, 2).alias("city_prefix"))
)

result = (
    valid
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.3, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
    )
)
