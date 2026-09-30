from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region").orderBy(F.col("region_avg_sqft").desc_nulls_last())
w3 = Window.partitionBy("region").orderBy(F.col("region_avg_sqft").asc_nulls_last())

base = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

kept_keys = (
    base
    .select("region", "id")
    .intersectAll(
        df_stores
        .filter(F.col("region").isin(["South", "East", "Central"]))
        .select("region", "id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .filter(F.col("store_name").like("%Flagship%"))
    .select("*", F.avg("sqft").over(w).alias("region_avg_sqft"))
)

scored_stores = (
    scored
    .select(
        "*",
        F.rank().over(w2).alias("region_avg_sqft_rank"),
        F.rank().over(w3).alias("region_avg_sqft_rank_2"),
        F.sum("region_avg_sqft").over(w).alias("region_sum_region_avg_sqft"),
    )
)

valid = (
    scored_stores
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        (F.col("region_sum_region_avg_sqft") * F.col("region_avg_sqft")).alias("region_sum_region_avg_sqft_times_region_avg_sqft"),
        F.round((F.col("region_avg_sqft") / F.col("region_sum_region_avg_sqft")), 2).alias("region_avg_sqft_per_region_sum_region_avg_sqft"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

valid_stores = (
    valid
    .filter((~F.col("region").eqNullSafe(F.lit("West"))) & (F.col("region_sum_region_avg_sqft_times_region_avg_sqft") <= 23))
    .select(
        "*",
        F.coalesce("region_sum_region_avg_sqft_times_region_avg_sqft", F.lit(0)).alias("region_sum_region_avg_sqft_times_region_avg_sqft_filled"),
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
    )
)

eligible = (
    valid_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.opened_date_period")), "left_anti")
)

grouped = (
    eligible
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("region_sum_region_avg_sqft")).otherwise(0)).alias("east_region_sum_region_avg_sqft"),
        F.max("opened_date_period").alias("last_seen"),
        F.percentile_approx("region_avg_sqft", 0.5).alias("region_avg_sqft_p50"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
    )
)

result = (
    grouped
    .select("week_start", "east_region_sum_region_avg_sqft", "sqft_bit_or")
    .orderBy("week_start", "east_region_sum_region_avg_sqft")
)
