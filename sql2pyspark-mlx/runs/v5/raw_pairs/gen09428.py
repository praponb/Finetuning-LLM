from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.year("opened_date") >= 2023)
    .withColumn("clean_store_name", F.trim("store_name"))
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 14764, "High").when(F.col("sqft") >= 4051, "Medium").otherwise("Low"))
    .withColumn("region_filled", F.coalesce("region", F.lit("UNKNOWN")))
)

cleaned = (
    base
    .filter(F.col("region").isNotNull() & (F.col("opened_date") <= "2023-04-01"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "region_filled",
        F.concat_ws(",", "city", "store_name").alias("city_store_name_key"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

filtered = (
    cleaned
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "city_store_name_key",
        F.col("sqft").cast("string").alias("sqft_str"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

valid = (
    filtered
    .filter(F.col("region").isNotNull() & (F.col("region").eqNullSafe(F.lit("Central"))))
    .select(
        "*",
        F.round((F.col("sqft") * 0.9), 2).alias("adjusted_sqft"),
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)

stats = (
    valid
    .groupBy(
        F.when(F.col("sqft") >= 16545, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        F.when(F.col("adjusted_sqft") >= 74, "PREMIUM").otherwise("STANDARD").alias("adjusted_sqft_tier"),
    )
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("adjusted_sqft").alias("min_adjusted_sqft"),
        F.count("*").alias("cnt"),
    )
)

result = (
    stats
    .select("sqft_tier", "adjusted_sqft_tier", "sqft_stddev", "event_count")
    .orderBy("event_count", "adjusted_sqft_tier")
)
