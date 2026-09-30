from pyspark.sql import functions as F

joined = (
    df_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .filter(F.length(F.col("p.product_name")) > 12)
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.quantity",
        F.col("p.price"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
)

result = (
    joined
    .withColumn("price_tier", F.when(F.col("price") >= 990, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["product_id", "price_tier"], ["product_id"], []],
        "product_id",
        "price_tier",
    )
    .agg(
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_count"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("quantity_bit_or").desc_nulls_first())
)
