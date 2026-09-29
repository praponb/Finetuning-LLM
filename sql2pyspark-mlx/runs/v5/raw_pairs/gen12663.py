from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("Central"))))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.substring("city", 1, 4).alias("city_prefix"),
    )
)

kept_keys = (
    cleaned
    .select("city")
    .intersectAll(
        df_stores
        .filter(~F.col("region").isin(["Central", "North"]))
        .select("city")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

cleaned_stores = (
    reconciled
    .filter((F.col("opened_date") > F.lit("2023-03-11").cast("date")) & F.col("region").isNotNull())
    .withColumn("store_name_normalized", F.upper(F.trim("store_name")))
)

eligible = (
    cleaned_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.opened_date")), "left_semi")
)

filtered = (
    eligible
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-05-26")))
    .select(
        "store_name",
        "city",
        "region",
        "opened_date",
        "city_prefix",
        "store_name_normalized",
        F.concat_ws(",", "region", "city").alias("region_city_key"),
    )
)

result = (
    filtered
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("region") == "East").alias("east_events"),
    )
)
