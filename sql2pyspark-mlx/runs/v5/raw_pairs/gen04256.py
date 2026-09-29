from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("product_id", "order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("order_id") == 6)
        .select("product_id", "order_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("unit_price") >= 310, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price_2"),
    )
)
