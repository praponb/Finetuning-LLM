from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isin([2, 14, 21, 29]))
        .select("id", "order_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.shipping_fee"))
)

valid = (
    enriched
    .filter(F.col("shipping_fee") <= 12)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "line_total",
        "shipping_fee",
        F.when(F.col("unit_price") >= 370, "High").when(F.col("unit_price") >= 60, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

summary = (
    valid
    .groupBy("product_id", "order_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    summary
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "product_id",
        "order_id",
        "distinct_ids",
        "distinct_order_ids",
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("product_id").desc_nulls_first())
)
