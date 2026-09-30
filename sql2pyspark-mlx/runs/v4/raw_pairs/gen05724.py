from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
        "order_id",
    )
    .agg(
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95_2"),
    )
    .orderBy(F.col("quantity_bit_and").desc_nulls_last())
)
