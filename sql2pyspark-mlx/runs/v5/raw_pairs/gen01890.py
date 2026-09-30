from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity_rounded").desc(), "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") > "2024-09-26")
    .select(
        "id",
        "store_id",
        "quantity",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id")
)

joined = (
    deduped
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.city") == "Tokyo")
    .select(
        "e.id",
        "e.store_id",
        F.col("d.store_name"),
        F.col("d.city"),
        F.col("d.sqft"),
    )
)

base = (
    joined
    .filter(F.col("city") == "Chiang Mai")
    .select("id", "store_id", "city", F.substring("city", 1, 2).alias("city_prefix"))
)

with_dims = (
    base
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.city").alias("store_city"))
)

valid = (
    with_dims
    .filter(F.col("city").isNotNull() & F.col("city").isNotNull())
    .withColumn("city_store_city_key", F.concat_ws("-", "city", "store_city"))
    .withColumn("store_city_group", F.when(F.col("store_city").isin(["Chiang Mai", "Singapore"]), "Core").otherwise("Other"))
    .withColumn("resolved_store_city", F.coalesce("store_city", F.lit("DIRECT")))
)

summary = (
    valid
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.count_if(F.col("store_city") == "New York").alias("new_york_events"),
        F.size(F.collect_set("store_city")).alias("num_store_cities"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("store_city")).alias("store_city_list"),
    )
)

result = (
    summary
    .select(
        "city",
        "tokyo_count",
        "new_york_events",
        "num_store_cities",
        "berlin_rows",
        "event_count",
        "store_city_list",
    )
    .orderBy(F.col("berlin_rows").desc_nulls_last())
)
