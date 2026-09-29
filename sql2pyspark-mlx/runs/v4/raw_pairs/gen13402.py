from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel", "status").orderBy("discount")
w4 = Window.partitionBy("customer_id", "status")
w5 = Window.partitionBy("customer_id", "channel").orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("discount_rank", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("shipping_fee", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("amount", "id", "promo_id")
w9 = Window.partitionBy("customer_id", "store_id").orderBy("amount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("promo_code")

attributed = (
    df_orders
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_windows = (
    attributed
    .select(
        "*",
        F.min("discount").over(w).alias("store_id_min_discount"),
        F.last("promo_id", ignorenulls=True).over(w2).alias("last_known_promo_id"),
        F.rank().over(w3).alias("discount_rank"),
    )
)

with_windows_orders = (
    with_windows
    .select(
        "*",
        F.corr("shipping_fee", "discount").over(w4).alias("corr_shipping_fee_discount"),
        F.sum("amount").over(w5).alias("running_amount"),
        F.first("channel", ignorenulls=True).over(w6).alias("first_known_channel"),
        F.sum("store_id_min_discount").over(w7).alias("cumulative_store_id_min_discount"),
        F.lead("shipping_fee", 1).over(w8).alias("next_shipping_fee"),
    )
)

enriched = (
    with_windows_orders
    .select(
        "*",
        F.sum("last_known_promo_id").over(w9).alias("running_last_known_promo_id"),
        F.sum("discount_rank").over(w10).alias("promo_code_sum_discount_rank"),
    )
)

valid = (
    enriched
    .filter((F.col("order_date") >= F.lit("2023-12-07").cast("date")) & F.col("promo_code").isNotNull() & (F.col("amount") > 870))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "discount_pct",
        "last_known_promo_id",
        "discount_rank",
        "corr_shipping_fee_discount",
        "running_amount",
        "first_known_channel",
        "cumulative_store_id_min_discount",
        "next_shipping_fee",
        "running_last_known_promo_id",
        "promo_code_sum_discount_rank",
        F.least("next_shipping_fee", "running_last_known_promo_id").alias("least_next_shipping_fee_running_last_known_promo_id"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

valid_orders = (
    valid
    .filter((F.col("order_date") > F.lit("2024-02-01").cast("date")) & F.col("store_id").isin([2, 12, 20, 22]))
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "discount_pct",
        "last_known_promo_id",
        "discount_rank",
        "corr_shipping_fee_discount",
        "running_amount",
        "first_known_channel",
        "next_shipping_fee",
        "running_last_known_promo_id",
        "promo_code_sum_discount_rank",
        "least_next_shipping_fee_running_last_known_promo_id",
        "channel_clean",
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean_2"),
    )
)

result = (
    valid_orders
    .groupBy(
        F.when(F.col("least_next_shipping_fee_running_last_known_promo_id") >= 75, "LARGE").otherwise("SMALL").alias("least_next_shipping_fee_running_last_known_promo_id_tier"),
        F.when(F.col("amount") >= 1760, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.when(F.col("running_amount") >= 8860, "LARGE").otherwise("SMALL").alias("running_amount_tier"),
    )
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.sum(F.when(F.col("channel") == "web", F.col("running_amount"))).alias("web_total_running_amount"),
        F.round(F.avg("discount_rank"), 2).alias("avg_discount_rank"),
        F.round(F.avg("corr_shipping_fee_discount"), 2).alias("avg_corr_shipping_fee_discount"),
        F.round(F.avg("corr_shipping_fee_discount"), 2).alias("avg_corr_shipping_fee_discount_2"),
    )
    .orderBy(F.col("avg_corr_shipping_fee_discount_2").desc())
)
