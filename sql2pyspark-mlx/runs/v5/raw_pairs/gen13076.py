from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("id", "order_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 420, "HIGH").otherwise("LOW"))
    .cube("unit_price_tier")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.grouping("unit_price_tier").alias("grouping_id"),
    )
    .orderBy(F.col("quantity_bit_and").desc_nulls_first())
)
