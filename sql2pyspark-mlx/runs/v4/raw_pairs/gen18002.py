from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("channel").isNotNull() & (F.col("amount") < 410))
    .select(
        "id",
        "channel",
        "discount",
        "shipping_fee",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity") < 6), "left_semi")
)

remaining_keys = (
    kept
    .select("channel", "id")
    .subtract(
        df_orders
        .filter(F.col("discount") <= 0.17)
        .select("channel", "id")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "channel",
        "discount",
        "shipping_fee",
        "discount_filled",
        "resolved_channel",
    )
    .orderBy(F.col("channel").desc_nulls_first(), "discount_filled")
)
