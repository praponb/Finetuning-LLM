from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Electronics", "Fashion"])), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("rating") >= 4.5, "HIGH").otherwise("LOW").alias("rating_tier"),
        "city",
    )
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
        F.max(F.when(F.col("city") == "Tokyo", F.col("rating"))).alias("tokyo_max_rating"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .filter(F.col("de_count") < 1)
    .orderBy(F.col("rating_p50").desc_nulls_last())
)
