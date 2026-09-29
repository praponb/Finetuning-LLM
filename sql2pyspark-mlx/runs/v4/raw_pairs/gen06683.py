from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city")
w_running = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc_nulls_first())

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-05-20").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.upper("store_name").alias("store_name_upper"),
    )
)

matched_keys = (
    filtered
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("%Old%"))
        .select("city")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("city_max_sqft", F.max("sqft").over(w2))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w_running))
)

eligible = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.opened_date")), "left_semi")
)

kept_keys = (
    eligible
    .select("city", "id")
    .intersect(
        df_stores
        .filter(F.col("city") != "New York")
        .select("city", "id")
    )
)

kept_rows_stores = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    kept_rows_stores
    .groupBy("region")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)

result = (
    daily
    .select("region", "max_sqft", "region_list")
    .orderBy(F.col("max_sqft").desc_nulls_last())
)
