from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("city", "region")
    .intersect(
        df_stores
        .filter(F.col("region") == "West")
        .select("city", "region")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.min("sqft").alias("min_sqft"),
        F.max("sqft").alias("max_sqft"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft"))).alias("bangkok_total_sqft"),
    )
)
