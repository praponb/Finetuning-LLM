from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock_month_end").asc_nulls_last(), "id")

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-09-26").cast("date")) & (F.year("last_restock") >= 2024))
    .select(
        "id",
        "store_id",
        "product_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id")
)

enriched = (
    deduped
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.region"), F.col("b.city"))
)

enriched_inventory = (
    enriched
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city").alias("store_city"))
)

cleaned_inventory = (
    enriched_inventory
    .filter(F.col("store_city").isNotNull() & F.col("store_id").isin([1, 2, 3]))
    .select(
        "*",
        F.upper("region").alias("region_upper"),
        F.lower("city").alias("city_lower"),
    )
)

with_dims = (
    cleaned_inventory
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.region",
        "src.city_lower",
        F.col("p.store_name"),
    )
)

result = (
    with_dims
    .groupBy("region", "store_id")
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.size(F.collect_set("region")).alias("num_regions_2"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
    )
)
