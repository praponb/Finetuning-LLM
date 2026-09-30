from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier", "order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("cost", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("carrier").orderBy("cost_avg_3_prior", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("channel").orderBy(F.col("event_seq").asc_nulls_first())

enriched = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id") == 13)
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.shipped_date",
        "a.delivered_date",
        "a.cost",
        F.col("d.order_date"),
        F.col("d.shipping_fee"),
        F.col("d.channel"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.sum("shipping_fee").over(w2).alias("running_shipping_fee"),
        F.avg("cost").over(w3).alias("cost_avg_3_prior"),
    )
)

attributed = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("src.shipped_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_windows_shipments = (
    attributed
    .select(
        "id",
        "order_id",
        "order_date",
        "channel",
        "running_shipping_fee",
        "promo_id",
        "promo_code",
        F.sum("running_shipping_fee").over(w4).alias("cumulative_running_shipping_fee"),
        F.rank().over(w_running).alias("event_seq_rank"),
    )
)

result = (
    with_windows_shipments
    .cube(
        F.when(F.col("event_seq_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_rank_tier"),
        F.date_trunc("year", "order_date").alias("year_start"),
    )
    .agg(
        F.sum("cumulative_running_shipping_fee").alias("total_cumulative_running_shipping_fee"),
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("running_shipping_fee")).otherwise(0)).alias("vip_running_shipping_fee"),
        F.count("*").alias("cnt"),
        F.round(F.avg("event_seq_rank"), 2).alias("avg_event_seq_rank"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .filter(F.col("cnt") > 1)
)
