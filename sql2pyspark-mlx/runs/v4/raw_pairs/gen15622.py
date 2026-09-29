from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") * 1.07), 2).alias("unit_price_scaled"),
    )
)

result = (
    cleaned
    .cube(
        F.when(F.col("unit_price_scaled") >= 68, "LARGE").otherwise("SMALL").alias("unit_price_scaled_tier"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("unit_price_p90")
)
