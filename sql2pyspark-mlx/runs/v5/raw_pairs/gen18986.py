from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first(), "id")

valid = (
    df_store_sales
    .filter(F.col("store_id") == 5)
    .select(
        "*",
        F.round("revenue", 2).alias("revenue_rounded"),
        F.round("units", 2).alias("units_rounded"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.sale_date",
        "e.units_rounded",
        F.col("s.opened_date"),
        F.col("s.city"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.sqft"), F.col("b.store_name"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.city") == "Singapore")
    .select(
        "s.id",
        "s.store_id",
        "s.city",
        "s.sqft",
        "s.store_name",
        F.col("p.store_name").alias("store_store_name"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "city")
)

valid_store_sales = (
    latest
    .filter(F.col("city").isNotNull() & (F.col("city") != "Bangkok"))
    .select("*", F.trim("city").alias("clean_city"))
)

result = (
    valid_store_sales
    .groupBy("city")
    .agg(
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
)
