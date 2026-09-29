from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.length("country").alias("country_length"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("country_length") >= 83, "HIGH").otherwise("LOW").alias("country_length_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("event_count"),
        F.round(F.sum("country_length") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("country_length_per_jp"),
        F.round(F.sum("country_length") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("country_length_per_london"),
        F.percentile_approx("country_length", 0.5).alias("country_length_p50"),
    )
)
