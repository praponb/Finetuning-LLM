from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "unit_price",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.round((F.col("quantity") * 1.15), 2).alias("quantity_adjusted"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("unit_price") >= 380, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.percentile_approx("quantity_adjusted", [0.25, 0.5, 0.75]).alias("quantity_adjusted_quartiles"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy(F.col("total_unit_price").desc())
)
