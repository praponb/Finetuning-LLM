from pyspark.sql import functions as F

result = (
    df_suppliers
    .cube(
        "city",
        F.when(F.col("rating") >= 3.8, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.avg(F.when(F.col("country") == "DE", F.col("rating"))).alias("de_avg_rating"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.max(F.when(F.col("city") == "Singapore", F.col("rating"))).alias("singapore_max_rating"),
        F.countDistinct("city").alias("distinct_cities"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("rating_per_singapore"),
    )
    .orderBy(F.col("rating_per_singapore").desc_nulls_last())
)
