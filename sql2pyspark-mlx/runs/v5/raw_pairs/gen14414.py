from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("customer_id", "method").orderBy("promo_id", "id", "promo_id")
w3 = Window.orderBy("shipping_fee", "id", "promo_id")

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-04-05").cast("date")) & F.col("amount").between(1050, 1110))
    .select(
        "*",
        F.greatest("shipping_fee", "discount").alias("greatest_shipping_fee_discount"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        F.col("c.status").alias("payment_status"),
        F.col("c.method"),
    )
)

with_window = (
    with_latest
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("e.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

eligible = (
    with_window
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.to_date(F.col("c.paid_at")) >= "2025-01-01"), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "order_date",
        "discount",
        "shipping_fee",
        "promo_id",
        "discount_pct",
        F.lag("discount_pct").over(w_hist).alias("prev_discount_pct"),
        F.lead("promo_id").over(w_hist).alias("next_promo_id"),
        F.lag("promo_id").over(w_hist).alias("prev_promo_id"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.lag("promo_id", 1).over(w3).alias("prev_promo_id_2"),
    )
)

eligible_orders = (
    enriched
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([7, 10, 20, 23])), "left_anti")
)

result = (
    eligible_orders
    .groupBy(
        F.when(F.col("event_seq") >= 7, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        "promo_id",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("next_promo_id"), 2).alias("avg_next_promo_id"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.percentile_approx("promo_id", 0.95).alias("promo_id_p95"),
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
    )
)
