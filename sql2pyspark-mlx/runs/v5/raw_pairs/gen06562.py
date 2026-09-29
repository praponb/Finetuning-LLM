from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("sqft").desc_nulls_first())

enriched = (
    df_stores
    .filter(F.col("city") != "New York")
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.max("sqft").over(w_grp).alias("running_max_sqft"),
        F.sum("sqft").over(w_grp).alias("running_sum_sqft"),
        F.min("sqft").over(w_grp).alias("running_min_sqft"),
        F.percent_rank().over(w2).alias("sqft_percent_rank"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.quantity").between(141, 229)), "left_anti")
)

remaining_keys = (
    kept
    .select("id", "city")
    .intersect(
        df_stores
        .filter(F.col("store_name").like("%Rive%"))
        .select("id", "city")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    reconciled
    .select("city", "id")
    .subtract(
        df_stores
        .filter(~F.col("region").isin(["West", "South", "East"]))
        .select("city", "id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .groupBy(
        "city",
        F.when(F.col("sqft_percent_rank") >= 0.75, "PREMIUM").otherwise("STANDARD").alias("sqft_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("sqft_percent_rank", 0.5).alias("sqft_percent_rank_p50"),
        F.round(F.sum("running_min_sqft") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("running_min_sqft_per_singapore"),
        F.bit_or("sqft").alias("sqft_bit_or"),
    )
)

result = grouped.select("city", "sqft_percent_rank_tier", "running_min_sqft_per_singapore")
