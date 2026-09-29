from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("region").orderBy("sqft", "id")

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("opened_date") > F.lit("2023-06-05").cast("date")))
    .select(
        "*",
        F.coalesce("region", F.lit("unassigned")).alias("region_filled"),
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

base = (
    cleaned
    .filter((F.col("opened_date") >= F.lit("2023-07-19").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.upper("region").alias("region_upper"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled_2"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

enriched = (
    with_latest
    .filter(F.col("sqft") != 8919)
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "region_filled",
        "store_name_prefix",
        "resolved_region",
        "region_upper",
        "region_filled_2",
        F.lag("sqft").over(w_part).alias("prev_sqft"),
        F.lead("sqft").over(w_part).alias("next_sqft"),
        F.lag("quantity").over(w_part).alias("prev_quantity"),
        F.lead("quantity").over(w_part).alias("next_quantity"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_anti")
)

kept_keys = (
    kept
    .select("region")
    .exceptAll(
        df_stores
        .filter(F.col("sqft") > 12201)
        .select("region")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "city",
        F.when(F.col("prev_sqft") >= 16507, "LARGE").otherwise("SMALL").alias("prev_sqft_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles_2"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
    )
)
