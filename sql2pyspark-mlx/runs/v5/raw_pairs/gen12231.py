from pyspark.sql import functions as F

result = (
    df_order_items
    .cube(
        "order_id",
        F.when(F.col("quantity") >= 7, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .orderBy("quantity_p90")
)
