from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price").between(110, 250))
        .select("id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.order_id", "s.product_id", F.col("b.order_date"), F.col("b.discount"))
)

joined = (
    enriched
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.shipping_fee") <= 31)
    .select("e.*", F.col("d.amount"), F.col("d.discount").alias("order_discount"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.status"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("order_discount") >= 0.35, "HIGH").otherwise("LOW").alias("order_discount_tier"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.sum("discount").alias("total_discount"),
        F.max(F.when(F.col("status") == "CANCELLED", F.col("discount"))).alias("cancelled_max_discount"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("amount_per_completed"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
    )
)
