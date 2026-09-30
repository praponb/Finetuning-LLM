from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id").orderBy(F.col("is_bundle").asc_nulls_first())
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy("customer_id")
w_ordered_2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("promo", "customer_id")
w_grp = Window.orderBy(F.col("price_max_1h").desc_nulls_last())
w_part = Window.partitionBy("promo", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy(F.col("price").asc_nulls_last())

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

base = (
    lines
    .filter(F.to_date("created_at") >= "2024-03-25")
    .select(
        "*",
        F.when(F.col("promo") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
        F.coalesce("promo", F.lit("NONE")).alias("resolved_promo"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("is_bundle_cume_dist"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share_2"),
        F.max("price").over(w_ordered_2).alias("price_max_1h"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.count("pos").over(w_ordered_2).alias("pos_count_1h"),
        F.max("qty_share").over(w5).alias("promo_max_qty_share"),
        F.dense_rank().over(w_grp).alias("price_max_1h_dense_rank"),
        F.count("qty_share").over(w_part).alias("qty_share_count_30m"),
        F.dense_rank().over(w8).alias("price_dense_rank"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("is_bundle") >= 84, "HIGH").otherwise("LOW").alias("is_bundle_tier"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.round(F.sum("price_max_1h_dense_rank") / F.nullif(F.count(F.when(F.col("promo") == "SPRING10", 1)), F.lit(0)), 2).alias("price_max_1h_dense_rank_per_spring10"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
)

result = (
    grouped
    .select(
        "is_bundle_tier",
        "hour_window",
        "price_max_1h_dense_rank_per_spring10",
        "distinct_ids",
    )
    .orderBy(F.col("price_max_1h_dense_rank_per_spring10").asc_nulls_last())
)
