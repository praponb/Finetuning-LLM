from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("qty").desc())
w3 = Window.partitionBy("promo").orderBy(F.col("promo_count_qty").asc_nulls_first())
w4 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("promo").orderBy(F.col("qty_share_2").desc_nulls_first())
w_grp = Window.partitionBy("promo").orderBy(F.col("qty_share").desc_nulls_last())
w8 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.promo"),
    )
)

with_windows = (
    lines
    .withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w)), 4))
    .withColumn("promo_count_qty", F.count("qty").over(w))
)

metrics = (
    with_windows
    .withColumn("qty_dense_rank", F.dense_rank().over(w2))
    .withColumn("promo_count_qty_dense_rank", F.dense_rank().over(w3))
    .withColumn("promo_count_qty_sum_1h", F.sum("promo_count_qty").over(w4))
    .withColumn("qty_share_2", F.round((F.col("qty") / F.sum("qty").over(w)), 4))
    .withColumn("qty_max_2h", F.max("qty").over(w5))
)

base = (
    metrics
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("promo").isNotNull() & (F.col("qty_dense_rank") > 7))
    .select(
        "id",
        "created_at",
        "product_id",
        "qty",
        "promo",
        "qty_share",
        "promo_count_qty",
        "qty_dense_rank",
        "promo_count_qty_dense_rank",
        "qty_share_2",
        F.ceil("qty").alias("qty_whole"),
        F.coalesce("promo", F.lit("DIRECT")).alias("promo_clean"),
    )
)

filtered = (
    base
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & F.col("promo").isNotNull())
    .withColumn("product_id_per_qty", F.round((F.col("product_id") / F.col("qty")), 2))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("DIRECT")))
)

enriched = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w6).alias("qty_share_2_percent_rank"),
        F.percent_rank().over(w_grp).alias("qty_share_percent_rank"),
        F.sum("qty_share_2").over(w).alias("promo_sum_qty_share_2"),
        F.count("promo_count_qty").over(w5).alias("promo_count_qty_count_2h"),
        F.round((F.col("promo_count_qty") / F.sum("promo_count_qty").over(w)), 4).alias("promo_count_qty_share"),
    )
)

enriched_carts = (
    enriched
    .withColumn("product_id_per_qty_max_15m", F.max("product_id_per_qty").over(w8))
    .withColumn("promo_min_promo_count_qty", F.min("promo_count_qty").over(w))
    .withColumn("promo_sum_qty_share_percent_rank", F.sum("qty_share_percent_rank").over(w))
    .withColumn("promo_min_qty_share_2_percent_rank", F.min("qty_share_2_percent_rank").over(w))
    .withColumn("promo_count_qty_share_2", F.round((F.col("promo_count_qty") / F.sum("promo_count_qty").over(w)), 4))
)

result = (
    enriched_carts
    .groupBy(
        F.when(F.col("qty_share_2") >= 0.62, "LARGE").otherwise("SMALL").alias("qty_share_2_tier"),
    )
    .agg(
        F.round(F.sum("product_id_per_qty") / F.nullif(F.count(F.when(F.col("promo") == "BUNDLE", 1)), F.lit(0)), 2).alias("product_id_per_qty_per_bundle"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
        F.min("promo_count_qty").alias("min_promo_count_qty"),
    )
)
