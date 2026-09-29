from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        F.round("unit_price", 0).alias("unit_price_rounded"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel") != "web")
    .select("a.*", F.col("o.channel"), F.col("o.discount"))
)

filtered = (
    with_dims
    .filter(F.col("channel").isNotNull() & (F.col("unit_price_rounded") > 62))
    .select(
        "*",
        F.length("channel").alias("channel_length"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

with_dims_order_items = (
    filtered
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select("a.*", F.col("o.amount"))
)

enriched = (
    with_dims_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.unit_price_rounded",
        "e.quantity_rounded",
        "e.discount",
        "e.channel_length",
        "e.resolved_channel",
        "e.amount",
        F.col("p.cost"),
        F.col("p.price"),
        F.col("p.product_name"),
    )
)

result = (
    enriched
    .select(
        "id",
        "product_id",
        "quantity_rounded",
        "discount",
        "resolved_channel",
        "cost",
        "price",
    )
    .orderBy(F.col("product_id").desc())
)
