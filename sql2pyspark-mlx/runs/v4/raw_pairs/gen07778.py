from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("order_id") == 40)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.col("quantity").cast("string").alias("quantity_str"),
        F.when(F.col("unit_price") >= 420, "High").when(F.col("unit_price") >= 150, "Medium").otherwise("Low").alias("unit_price_tier"),
    )
)

result = (
    valid
    .cube("order_id", "product_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.percentile_approx("unit_price", 0.5).alias("unit_price_p50"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
    )
    .filter(F.col("total_unit_price") < 0)
    .orderBy("product_id")
)
