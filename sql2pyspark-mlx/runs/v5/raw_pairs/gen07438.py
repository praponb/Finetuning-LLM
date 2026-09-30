from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("city") == "Berlin")
    .select(
        "*",
        F.trim("city").alias("clean_city"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

matched_keys = (
    base
    .select("region", "id")
    .subtract(
        df_stores
        .filter(F.col("opened_date") >= "2025-10-29")
        .select("region", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 3, 4])), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("region").isNotNull() & F.col("opened_date").between("2023-04-01", "2024-04-17"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "resolved_region",
        F.ceil("sqft").alias("sqft_whole"),
    )
)

filtered_stores = (
    filtered
    .filter((F.col("opened_date") >= F.lit("2023-03-24").cast("date")) & (F.col("region").eqNullSafe(F.lit("South"))))
    .withColumn("sqft_minus_sqft_whole", (F.col("sqft") - F.col("sqft_whole")))
)

result = (
    filtered_stores
    .groupBy("region")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("sqft_minus_sqft_whole")).otherwise(0)).alias("tokyo_sqft_minus_sqft_whole"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .filter(F.col("bangkok_count") < 1000)
)
