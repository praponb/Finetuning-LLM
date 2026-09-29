from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.concat_ws("-", "country", "city").alias("country_city_key"),
        F.round((F.col("rating") * 0.85), 2).alias("rating_adjusted"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("rating") >= 4.4, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.min("rating_adjusted").alias("min_rating_adjusted"),
    )
)
