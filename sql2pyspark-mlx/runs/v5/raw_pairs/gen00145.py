from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

valid = (
    filtered
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_band", F.when(F.col("unit_price") >= 340, "High").when(F.col("unit_price") >= 180, "Medium").otherwise("Low"))
)

kept_keys = (
    valid
    .select("order_id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") > 7)
        .select("order_id", "product_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("product_id").isNotNull() & (F.col("order_id") == 30))
    .select(
        "id",
        "unit_price",
        "unit_price_band",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        (F.col("quantity") + F.col("line_total")).alias("quantity_plus_line_total"),
    )
)

result = (
    cleaned
    .select("id", "quantity_filled", "quantity_plus_line_total")
    .orderBy(F.col("id").desc_nulls_first())
)
