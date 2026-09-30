from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.size(F.col("x.tags")) >= 2), "left_semi")
)

result = (
    not_flagged
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.0, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["rating_tier", "city"], ["rating_tier"]], "rating_tier", "city")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
    )
    .orderBy(F.col("event_count").desc())
)
