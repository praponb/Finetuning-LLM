from pyspark.sql import functions as F

revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("revenue").alias("max_store_sale_revenue"))

joined = (
    df_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.brand"))
)

matched_keys = (
    joined
    .select("order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity") >= 6)
        .select("order_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("brand").isNotNull())
    .withColumn("quantity_whole", F.ceil("quantity"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("OTHER")))
)

filtered = (
    valid
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") == 2))
    .select(
        "*",
        F.when(F.col("quantity_whole") >= 64, "High").when(F.col("quantity_whole") >= 14, "Medium").otherwise("Low").alias("quantity_whole_tier"),
        F.lower("brand").alias("brand_lower"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

matched_keys_2 = (
    filtered
    .select("id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id") == 7)
        .select("id")
    )
)

verified_order_items = (
    filtered
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified_order_items
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_whole",
        "brand_filled",
        "brand_lower",
        "brand_clean",
        F.col("max_store_sale_revenue"),
    )
    .orderBy("product_id")
)
