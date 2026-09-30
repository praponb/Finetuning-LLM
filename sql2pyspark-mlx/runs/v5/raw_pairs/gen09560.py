from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isin([8, 9, 14]))
    .select(
        "*",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("line_total") >= 80, "HIGH").otherwise("LOW").alias("line_total_tier"),
        "order_id",
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.avg("quantity").alias("avg_quantity"),
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
    )
    .orderBy(F.col("unit_price_p95").desc())
)
