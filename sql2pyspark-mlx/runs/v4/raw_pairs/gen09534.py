from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.orderBy(F.col("sqft_avg_3").desc_nulls_first())
w5 = Window.partitionBy("city").orderBy(F.col("sqft_avg_3").desc_nulls_first())
w6 = Window.orderBy(F.col("sqft_avg_3_cume_dist").desc_nulls_last())
w7 = Window.partitionBy("city", "region")
w8 = Window.partitionBy("region", "city")

with_windows = (
    df_stores
    .select(
        "*",
        F.last("sqft", ignorenulls=True).over(w).alias("last_known_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_3"),
    )
)

matched_keys = (
    with_windows
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("M%"))
        .select("region", "city")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.cume_dist().over(w4).alias("sqft_avg_3_cume_dist"),
        F.dense_rank().over(w5).alias("sqft_avg_3_dense_rank"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w6).alias("sqft_avg_3_cume_dist_dense_rank"),
        F.round((F.col("sqft") / F.sum("sqft").over(w7)), 4).alias("sqft_share_2"),
        F.round((F.col("last_known_sqft") / F.sum("last_known_sqft").over(w8)), 4).alias("last_known_sqft_share"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock").between("2023-03-01", "2024-01-24")), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("sqft_share_2") >= 0.81, "LARGE").otherwise("SMALL").alias("sqft_share_2_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        "city",
    )
    .agg(
        F.countDistinct("region").alias("distinct_regions"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("last_known_sqft"))).alias("singapore_total_last_known_sqft"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy(F.col("week_start").asc_nulls_first())
)
