from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("rating") < 3.8)
        .select("id")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.6, "LARGE").otherwise("SMALL"))
    .groupingSets([["rating_tier", "city"], ["rating_tier"]], "rating_tier", "city")
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
    )
)
