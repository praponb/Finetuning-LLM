from pyspark.sql import functions as F

remaining_keys = (
    df_stores
    .select("region", "id")
    .exceptAll(
        df_stores
        .filter(F.col("opened_date") <= "2025-05-01")
        .select("region", "id")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("week", "opened_date").alias("week_start"), "city")
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "West", 1)), F.lit(0)), 2).alias("sqft_per_west"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft")).otherwise(0)).alias("chiang_mai_sqft"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("sqft_per_berlin"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
    )
    .orderBy(F.col("sqft_per_west").asc_nulls_last())
)
