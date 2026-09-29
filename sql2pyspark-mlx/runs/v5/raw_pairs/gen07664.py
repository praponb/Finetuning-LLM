from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("channel").orderBy("order_date", "id")
w_running = Window.partitionBy("store_id").orderBy("promo_id", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("promo_code").orderBy("prev_discount", "id", "promo_id").rowsBetween(-3, -1)

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-12").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        F.round((F.col("amount") * 0.9), 2).alias("adjusted_amount"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([16, 25, 27, 30])), "left_anti")
)

with_windows = (
    eligible
    .select(
        "*",
        F.avg("adjusted_amount").over(w).alias("adjusted_amount_avg_3_prior"),
        F.lag("discount", 1).over(w2).alias("prev_discount"),
    )
)

attributed = (
    with_windows
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") <= F.date_add(F.col("pr.ends_on"), 7)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

enriched = (
    attributed
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        "shipping_fee",
        "adjusted_amount",
        "promo_id",
        "promo_code",
        F.avg("promo_id").over(w_running).alias("promo_id_avg_6"),
        F.avg("adjusted_amount").over(w_grp).alias("adjusted_amount_avg_3_prior_2"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("MONTH", "order_date").alias("activity_month"))
    .agg(
        F.min("discount").alias("min_discount"),
        F.percentile_approx("shipping_fee", 0.5).alias("shipping_fee_p50"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.sum("promo_id_avg_6").alias("total_promo_id_avg_6"),
        F.max(F.when(F.col("promo_code") == "VIP", F.col("promo_id"))).alias("vip_max_promo_id"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .orderBy(F.col("min_discount").desc_nulls_first())
)
