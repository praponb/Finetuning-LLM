from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("rating_whole", F.ceil("rating"))
    .withColumn("city_filled", F.coalesce("city", F.lit("OTHER")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("rating_whole") >= 85, "LARGE").otherwise("SMALL").alias("rating_whole_tier"),
    )
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("rating_per_new_york"),
        F.percentile_approx("rating_whole", 0.95).alias("rating_whole_p95"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
    )
    .orderBy(F.col("rating_whole_p95").asc_nulls_first())
)
