from pyspark.sql import functions as F

remaining_keys = (
    df_stores
    .select("region", "city")
    .exceptAll(
        df_stores
        .filter(F.col("city") == "Tokyo")
        .select("region", "city")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("region")
    .agg(
        F.count_if(F.col("region") == "South").alias("south_events"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50_2"),
        F.count_if(F.col("region") == "South").alias("south_events_2"),
    )
)
