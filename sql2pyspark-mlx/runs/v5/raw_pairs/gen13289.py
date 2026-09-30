from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-08-03").cast("date")) & (F.col("sqft") != 6240))
    .select("*", F.initcap(F.trim("city")).alias("city_title"))
)

kept_keys = (
    valid
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.col("opened_date") > "2024-05-28")
        .select("city", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id").isin([1, 6, 8])), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id") == 21), "left_semi")
)

result = (
    eligible
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.max("sqft").alias("max_sqft"),
        F.count_if(F.col("region") == "North").alias("north_events"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
    )
    .filter(F.col("max_sqft") < 1000000)
    .orderBy(F.col("num_regions").desc())
)
