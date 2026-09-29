from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("region")
w4 = Window.partitionBy("region", "city")
w5 = Window.partitionBy("city").orderBy(F.col("sqft_share_rounded_p50").desc())

deduped = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "region", "opened_date", "sqft")
)

matched_keys = (
    deduped
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("region") == "North")
        .select("city")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .filter(F.col("sqft") != 10590)
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.count("sqft").over(w3).alias("region_count_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share_2"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.year(F.col("x.order_date")) == 2023), "left_anti")
)

scored = (
    eligible
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4).alias("sqft_share_3"),
    )
)

base = (
    scored
    .filter((F.col("opened_date") >= F.lit("2023-05-03").cast("date")) & F.col("region").isin(["North", "Central"]))
    .select(
        "id",
        "city",
        "region",
        "sqft_share",
        "region_count_sqft",
        "sqft_share_2",
        F.round("sqft_share", 0).alias("sqft_share_rounded"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") < "2025-03-01"), "left_anti")
)

result = (
    kept
    .groupBy(
        "city",
        F.when(F.col("sqft_share_2") >= 0.89, "LARGE").otherwise("SMALL").alias("sqft_share_2_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("sqft_share_rounded", 0.5).alias("sqft_share_rounded_p50"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
        F.round(F.sum("sqft_share_rounded") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("sqft_share_rounded_per_south"),
    )
    .withColumn("sqft_share_rounded_p50_dense_rank", F.dense_rank().over(w5))
    .orderBy(F.col("west_rows").asc_nulls_first())
)
