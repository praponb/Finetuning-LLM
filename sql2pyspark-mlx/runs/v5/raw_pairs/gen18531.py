from pyspark.sql import functions as F

unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

filtered = (
    df_order_items
    .filter(F.col("unit_price") >= 110)
    .withColumn("adjusted_unit_price", F.round((F.col("unit_price") * 1.1), 2))
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

kept_keys = (
    filtered
    .select("product_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isin([8, 17, 23, 37]))
        .select("product_id", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.order_id",
        "e.product_id",
        "e.unit_price",
        "e.quantity_tier",
        F.col("o.status"),
        F.col("o.amount"),
        F.col("o.discount"),
    )
)

joined_order_items = (
    joined
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.category") != "Grocery")
    .select("s.*", F.col("b.brand"), F.col("b.product_name"), F.col("b.stock_qty"))
)

remaining_keys = (
    joined_order_items
    .select("order_id", "product_id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price") > 210)
        .select("order_id", "product_id")
    )
)

reconciled_order_items = (
    joined_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

filtered_order_items = (
    reconciled_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "order_id",
        "unit_price",
        "quantity_tier",
        "status",
        "brand",
        "stock_qty",
        F.initcap(F.trim("product_name")).alias("product_name_title"),
        F.lower("brand").alias("brand_lower"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

result = (
    filtered_order_items
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "unit_price",
        "quantity_tier",
        "status",
        "brand",
        "product_name_title",
        "brand_lower",
        "status_filled",
        F.when(F.col("brand").rlike("^(Initech|Stark)$"), "CORE").when(F.col("brand").rlike("^(Acme|Globex)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("brand_zone"),
        F.col("min_order_item_unit_price"),
    )
    .orderBy(F.col("min_order_item_unit_price").asc_nulls_last())
)
