from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").asc_nulls_last())
w_recent = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("shipping_fee").asc_nulls_first())
w4 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy("customer_id")
w6 = Window.partitionBy("channel")
w_part = Window.partitionBy("store_id").orderBy("corr_amount_shipping_fee", "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-06-01"))) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .withColumn("channel_status_key", F.concat_ws("/", "channel", "status"))
    .withColumn("status_channel", F.concat("status", F.lit(", "), "channel"))
)

deduped = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .filter(F.col("shipping_fee") >= 30)
    .select(
        "*",
        F.last("shipping_fee", ignorenulls=True).over(w_recent).alias("last_known_shipping_fee"),
        F.rank().over(w3).alias("shipping_fee_rank"),
        F.avg("amount").over(w4).alias("amount_avg_5_prior"),
        F.corr("amount", "shipping_fee").over(w_running).alias("corr_amount_shipping_fee"),
        F.round((F.col("amount") / F.sum("amount").over(w6)), 4).alias("amount_share"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id") == 31), "left_semi")
)

with_windows = (
    not_flagged
    .withColumn("prev_amount_avg_5_prior", F.lag("amount_avg_5_prior").over(w_part))
    .withColumn("next_shipping_fee", F.lead("shipping_fee").over(w_part))
    .withColumn("event_seq", F.row_number().over(w_part))
)

kept_keys = (
    with_windows
    .select("store_id")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee") < 35)
        .select("store_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "customer_id",
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.count("*").alias("row_count"),
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
        F.sum(F.when(F.col("channel") == "web", F.col("corr_amount_shipping_fee")).otherwise(0)).alias("web_corr_amount_shipping_fee"),
    )
    .filter(F.col("web_corr_amount_shipping_fee") > 0)
    .orderBy("shipped_events")
)
