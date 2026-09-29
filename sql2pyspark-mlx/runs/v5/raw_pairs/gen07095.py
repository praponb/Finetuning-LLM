from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("country", "id")
    .exceptAll(
        df_suppliers
        .filter(F.col("city").isNotNull())
        .select("country", "id")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("rating").alias("total_rating"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating")).otherwise(0)).alias("tokyo_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("rating_per_tokyo"),
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
    )
)
