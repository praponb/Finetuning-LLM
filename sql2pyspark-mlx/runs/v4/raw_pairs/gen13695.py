from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.sum("quantity").alias("total_quantity"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("event_count"),
    )
    .orderBy("event_count")
)
