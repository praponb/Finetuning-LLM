from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["product_id", "quantity_tier", "order_id"], ["product_id", "quantity_tier"], ["product_id"], []],
        "product_id",
        "quantity_tier",
        "order_id",
    )
    .agg(
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("unit_price", 0.5).alias("unit_price_p50"),
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95"),
        F.grouping("product_id").alias("grouping_level"),
    )
    .orderBy("unit_price_p50")
)
