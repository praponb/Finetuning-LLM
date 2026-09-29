from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").like("%Trad%"))
    .select(
        "*",
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled_2"),
    )
)

result = (
    valid
    .groupBy("country")
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("rating_per_us"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("rating_per_bangkok"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
    )
    .filter(F.col("rating_per_us") < 0)
)
