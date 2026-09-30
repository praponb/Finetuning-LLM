from pyspark.sql import functions as F

unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

valid = (
    df_order_items
    .filter(F.col("product_id").isin([7, 12, 25]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.floor("unit_price").alias("unit_price_whole"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.quantity",
        "src.unit_price",
        "src.quantity_filled",
        "src.unit_price_whole",
        F.col("b.amount"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.discount"))
)

filtered = (
    enriched
    .filter(F.col("discount") >= 0.22)
    .select(
        "id",
        "order_id",
        "quantity",
        "quantity_filled",
        "unit_price_whole",
        "amount",
        F.col("unit_price").cast("int").alias("unit_price_int"),
        F.round("discount", 2).alias("discount_rounded"),
    )
)

result = (
    filtered
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "quantity_filled",
        "unit_price_whole",
        "amount",
        "unit_price_int",
        "discount_rounded",
        F.col("min_order_item_unit_price"),
    )
)
