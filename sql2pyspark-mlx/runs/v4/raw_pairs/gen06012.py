from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([13, 39]))
    .select(
        "*",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

result = (
    base
    .rollup("product_id")
    .agg(
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
