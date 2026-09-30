from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-06"))) & F.col("region").isNotNull() & (F.col("city") == "Berlin"))
    .select(
        "*",
        F.concat_ws("/", "city", "store_name").alias("city_store_name_key"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

remaining_keys = (
    with_latest
    .select("region", "city")
    .intersectAll(
        df_stores
        .filter(F.col("sqft") < 7592)
        .select("region", "city")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

daily = (
    kept_rows
    .groupBy(
        F.when(F.col("quantity") >= 267, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.sum(F.when(F.col("region") == "South", F.col("quantity")).otherwise(0)).alias("south_quantity"),
    )
    .filter(F.col("sqft_p50") < 0)
)

result = (
    daily
    .select("quantity_tier", "num_regions", "sqft_p50", "south_quantity")
    .orderBy(F.col("num_regions").desc())
)
