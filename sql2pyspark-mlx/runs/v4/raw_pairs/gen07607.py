from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "region").orderBy(F.col("cumulative_quantity").desc(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("cumulative_quantity").desc_nulls_last(), "id")

enriched = (
    df_inventory
    .withColumn("cumulative_quantity", F.sum("quantity").over(w))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w2))
)

joined = (
    enriched
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.city"), F.col("p.sqft"), F.col("p.region"))
)

ranked = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched_inventory = (
    latest
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.region").alias("store_region"))
)

daily = (
    enriched_inventory
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 17089, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "last_restock"))
    .groupingSets(
        [["sqft_tier", "week_start"], ["sqft_tier"], []],
        "sqft_tier",
        "week_start",
    )
    .agg(
        F.round(F.sum("cumulative_quantity") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("cumulative_quantity_per_berlin"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("cumulative_quantity"), 2).alias("avg_cumulative_quantity"),
        F.percentile_approx("last_known_quantity", [0.25, 0.5, 0.75]).alias("last_known_quantity_quartiles"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("store_region") == "Central", 1)), F.lit(0)), 2).alias("quantity_per_central"),
    )
)

result = (
    daily
    .select(
        "week_start",
        "cumulative_quantity_per_berlin",
        "city_list",
        "avg_cumulative_quantity",
        "last_known_quantity_quartiles",
    )
    .orderBy(F.col("week_start").desc())
)
