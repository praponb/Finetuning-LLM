from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("id", "channel")
    .intersect(
        df_orders
        .filter(F.col("channel") == "partner")
        .select("id", "channel")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_window = (
    kept_rows
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

kept = (
    with_window
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price").isNotNull()), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") >= "2025-11-01"), "left_anti")
)

valid = (
    eligible
    .filter(~F.col("channel").isin(["store", "web"]))
    .withColumn("discount_band", F.when(F.col("discount") >= 0.33, "High").when(F.col("discount") >= 0.09, "Medium").otherwise("Low"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("N/A")))
)

wide = (
    valid
    .groupBy("promo_code")
    .pivot("channel", ["store", "partner", "web"])
    .agg(F.max("discount_pct").alias("amount"), F.count(F.lit(1)).alias("n"))
)

result = (
    wide
    .select(
        "promo_code",
        "store_amount",
        "partner_amount",
        "partner_n",
        "web_amount",
        "web_n",
    )
    .orderBy("promo_code")
)
