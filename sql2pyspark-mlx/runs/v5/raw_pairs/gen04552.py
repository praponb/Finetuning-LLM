from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") > 4)
        .select("id", "product_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("unit_price") >= 430, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .filter(F.col("quantity_bit_and") >= 1)
)
