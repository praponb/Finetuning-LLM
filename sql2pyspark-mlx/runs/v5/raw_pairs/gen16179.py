from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("unit_price") > 310)
    .select(
        "*",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)

result = (
    valid
    .rollup(
        F.when(F.col("greatest_unit_price_quantity") >= 85, "HIGH").otherwise("LOW").alias("greatest_unit_price_quantity_tier"),
        "order_id",
    )
    .agg(
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)
