from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") >= 3.2)
        .select("country")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("rating") >= 4.5, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("avg_rating").desc())
)
