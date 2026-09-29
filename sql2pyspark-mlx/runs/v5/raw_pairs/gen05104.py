from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id", "promo_code")
w2 = Window.partitionBy("customer_id").orderBy("discount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.partitionBy("store_id").orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("discount_pct")
w5 = Window.partitionBy("store_id", "customer_id").orderBy("discount", "id", "promo_id").rowsBetween(-6, -1)
w_running = Window.partitionBy("store_id").orderBy("last_known_shipping_fee", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("promo_code", "status").orderBy("discount_pct_dense_rank", "id", "promo_id")

matched = (
    df_orders
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_windows = (
    matched
    .filter(F.col("discount_pct").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        F.corr("amount", "discount").over(w_recent).alias("corr_amount_discount"),
        F.last("shipping_fee", ignorenulls=True).over(w2).alias("last_known_shipping_fee"),
        F.sum("amount").over(w_recent_2).alias("cumulative_amount"),
        F.dense_rank().over(w4).alias("discount_pct_dense_rank"),
        F.avg("shipping_fee").over(w5).alias("shipping_fee_avg_6_prior"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "promo_id",
        "promo_code",
        "last_known_shipping_fee",
        "discount_pct_dense_rank",
        "shipping_fee_avg_6_prior",
        F.max("discount").over(w_running).alias("running_max_discount"),
        F.avg("discount").over(w_running).alias("running_avg_discount"),
        F.sum("discount").over(w_running).alias("running_sum_discount"),
    )
)

base = (
    enriched
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct_dense_rank") < 7))
    .select(
        "*",
        F.trim("channel").alias("clean_channel"),
        F.greatest("promo_id", "last_known_shipping_fee").alias("greatest_promo_id_last_known_shipping_fee"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") <= F.col("s.order_date")), "left_anti")
)

enriched_orders = (
    not_flagged
    .select(
        "id",
        "order_date",
        "discount",
        "promo_id",
        "last_known_shipping_fee",
        "discount_pct_dense_rank",
        "clean_channel",
        "greatest_promo_id_last_known_shipping_fee",
        "resolved_channel",
        F.lead("last_known_shipping_fee").over(w_grp).alias("next_last_known_shipping_fee"),
        F.lag("last_known_shipping_fee").over(w_grp).alias("prev_last_known_shipping_fee"),
        F.lag("shipping_fee_avg_6_prior").over(w_grp).alias("prev_shipping_fee_avg_6_prior"),
        F.lead("shipping_fee_avg_6_prior").over(w_grp).alias("next_shipping_fee_avg_6_prior"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

result = (
    enriched_orders
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.percentile_approx("last_known_shipping_fee", 0.5).alias("last_known_shipping_fee_p50"),
        F.max("greatest_promo_id_last_known_shipping_fee").alias("max_greatest_promo_id_last_known_shipping_fee"),
        F.percentile_approx("discount_pct_dense_rank", [0.25, 0.5, 0.75]).alias("discount_pct_dense_rank_quartiles"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.percentile_approx("greatest_promo_id_last_known_shipping_fee", [0.25, 0.5, 0.75]).alias("greatest_promo_id_last_known_shipping_fee_quartiles"),
    )
    .filter(F.col("event_seq_p50") > 0)
    .orderBy(F.col("last_known_shipping_fee_p50").desc())
)
