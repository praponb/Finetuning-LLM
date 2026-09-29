from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("city", "id")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("T%"))
        .select("city", "id")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("rating") >= 3.8, "HIGH").otherwise("LOW").alias("rating_tier"),
        "country",
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("rating_p95") > 0)
    .orderBy(F.col("max_rating").asc_nulls_last())
)
