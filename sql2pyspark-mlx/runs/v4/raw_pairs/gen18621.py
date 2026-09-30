from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 310, "HIGH").otherwise("LOW").alias("unit_price_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("distinct_ids")
)
