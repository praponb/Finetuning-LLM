from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.col("opened_date") >= "2023-06-06")
        .select("region", "city")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 13331, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "sqft_tier"], ["quarter_start"], []],
        "quarter_start",
        "sqft_tier",
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count("*").alias("event_count"),
        F.max(F.when(F.col("city") == "Bangkok", F.col("sqft"))).alias("bangkok_max_sqft"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft"))).alias("berlin_total_sqft"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
    )
    .orderBy(F.col("event_count").desc())
)
