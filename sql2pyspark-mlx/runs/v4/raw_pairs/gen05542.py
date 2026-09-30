from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.year("opened_date") == 2025)
    .select(
        "*",
        F.floor("sqft").alias("sqft_whole"),
        F.concat("region", F.lit(", "), "city").alias("region_city"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

valid = (
    filtered
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-11"))) & F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft_whole",
        "region_city",
        F.floor("sqft").alias("sqft_whole_2"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

valid_stores = (
    valid
    .filter(F.col("sqft_whole_2") >= 26)
    .select("*", F.lower("city").alias("city_lower"))
)

base = (
    valid_stores
    .filter((F.col("opened_date") >= F.lit("2023-03-25").cast("date")) & F.col("region").isNotNull())
    .select("*", F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"))
)

summary = (
    base
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("sqft_whole_2", 0.95).alias("sqft_whole_2_p95"),
        F.percentile_approx("sqft_whole", 0.5).alias("sqft_whole_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = (
    summary
    .select(
        "quarter_start",
        "sqft_whole_2_p95",
        "sqft_whole_p50",
        "distinct_ids",
        "city_list",
    )
    .orderBy("sqft_whole_p50")
)
