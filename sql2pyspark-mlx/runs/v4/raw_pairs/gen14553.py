from pyspark.sql import functions as F

matched_keys = (
    df_suppliers
    .select("city")
    .exceptAll(
        df_suppliers
        .filter(F.col("city").isNotNull())
        .select("city")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("rating") >= 3.9, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
    )
    .orderBy(F.col("num_cities").asc_nulls_first())
)
